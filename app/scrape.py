import enum
import os
import time
from pathlib import Path
from typing import LiteralString, cast

import pydantic
from bencode2 import BencodeDecodeError
from sslog import logger

from app.config import Config
from app.const import SELECTED_CATEGORY
from app.db import Database
from app.mt import MTeamAPI, MTeamRequestError, TorrentFileError, httpx_network_errors
from app.torrent import parse_torrent
from app.utils import get_info_hash_v1_from_content, parse_obj_as

known_max_id = 1150590


class RunResult(enum.Enum):
    ok = "ok"
    rate_limited = "rate_limited"
    error = "error"


class Scrape:
    mteam_client: MTeamAPI
    __db: Database

    def __init__(self, c: Config):
        self.__db = Database(c)
        self.mteam_client = MTeamAPI(c)

        for sql_file in Path(__file__, "../sql/").resolve().iterdir():
            print(f"executing {sql_file.name}")
            self.__db.execute(cast(LiteralString, sql_file.read_text(encoding="utf-8")))

    def scrape(self, limit: int = 0) -> None:
        fetched_max_id = (
            self.__db.fetch_val("select tid from thread order by tid desc limit 1") or 1
        )

        current_count = 0

        for lo in range(fetched_max_id, known_max_id, 100):
            hi = min(lo + 99, known_max_id - 1)
            current_ids = {
                x[0]
                for x in self.__db.fetch_all(
                    """select tid from thread where tid >= $1 and tid <= $2""", [lo, hi]
                )
            }

            for i in (x for x in range(lo, hi + 1) if x not in current_ids):
                logger.info("fetch {}", i)
                try:
                    r = self.mteam_client.torrent_detail(i)
                except MTeamRequestError as e:
                    if e.message == "種子未找到":
                        self.__db.execute(
                            """
                        insert into thread (tid, deleted)
                        values ($1, true)
                        on conflict (tid) do update set deleted = true
                        """,
                            [i],
                        )
                        continue
                    raise

                self.__db.execute(
                    """
                    insert into thread (tid, size, mediainfo, category, seeders, deleted)
                    values ($1, $2, $3, $4, $5, false)
                    on conflict (tid) do update set
                    size = excluded.size,
                    category = excluded.category,
                    seeders = excluded.seeders,
                    deleted = false
                    """,
                    [
                        i,
                        r.size,
                        r.mediainfo or "",
                        r.category,
                        r.status.seeders,
                    ],
                )

                current_count += 1
                if limit and current_count >= limit:
                    return

    def fetch_torrent(self) -> bool:
        threads = self.__db.fetch_all(
            """
            select tid from thread
            where
              deleted = false and
              info_hash = '' and
              mediainfo = '' and
              seeders != 0 and
              category = any($1)
            order by seeders desc
            limit 50
            """,
            [SELECTED_CATEGORY],
        )

        if not threads:
            return True

        for (tid,) in threads:
            logger.info("fetch torrent of thread {}", tid)
            try:
                tc = self.mteam_client.download_torrent(tid=tid)
            except TorrentFileError:
                logger.warning("torrent file error for thread {}", tid)
                self.__db.execute(
                    """update thread set mediainfo = $2 where tid = $1""",
                    [tid, "invalid torrent"],
                )
                continue

            try:
                t = parse_torrent(tc)
            except (pydantic.ValidationError, BencodeDecodeError):
                logger.exception("failed to parse torrent of {}", tid)
                self.__db.execute(
                    """update thread set mediainfo = $2 where tid = $1""",
                    [tid, "invalid torrent"],
                )
                continue

            info_hash = get_info_hash_v1_from_content(tc)

            self.__db.execute(
                """
                    insert into torrent (tid, info_hash, content)
                    VALUES ($1, $2, $3)
                    on conflict (tid) do nothing
                    """,
                [tid, info_hash, tc],
            )

            self.__db.execute(
                """update thread set info_hash = $2, size = $3 where tid = $1""",
                [tid, info_hash, t.total_length],
            )
        return False

    @staticmethod
    def __is_rate_limited(e: MTeamRequestError) -> bool:
        return e.message in ("請求過於頻繁", "今日下載配額用盡")

    def __run_fetch(self) -> RunResult:
        """Returns (result, no_pending)."""
        try:
            self.fetch_torrent()
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to fetch torrents")
            return RunResult.error
        return RunResult.ok

    def __run_scrape(self, limit: int) -> RunResult:
        try:
            self.scrape(limit=limit)
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to fetch threads")
            return RunResult.error
        return RunResult.ok

    def __run(self) -> None:
        limit = parse_obj_as(int, os.environ.get("SCRAPE_LIMIT", "100"))
        while True:
            logger.info("fetch torrents")

            fetch_result = self.__run_fetch()
            scrape_result = self.__run_scrape(limit)

            if fetch_result == RunResult.rate_limited and scrape_result == RunResult.rate_limited:
                time.sleep(30 * 60)  # 30 minutes
            else:
                time.sleep(10 * 60)  # 10 minutes

    def start(self) -> None:
        self.__run()
