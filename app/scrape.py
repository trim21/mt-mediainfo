import itertools
import os
import time
from pathlib import Path

import pydantic
from bencode2 import BencodeDecodeError
from sslog import logger

from app.config import Config
from app.const import SELECTED_CATEGORY
from app.db import Database
from app.mt import MTeamAPI, MTeamRequestError, httpx_network_errors
from app.torrent import parse_torrent
from app.utils import get_info_hash_v1_from_content, parse_obj_as

known_max_id = 1150590


class Scrape:
    mteam_client: MTeamAPI
    __db: Database

    def __init__(self, c: Config):
        self.__db = Database(c)
        self.mteam_client = MTeamAPI(c)

        for sql_file in Path(__file__, "../sql/").resolve().iterdir():
            print(f"executing {sql_file.name}")
            self.__db.execute(sql_file.read_text(encoding="utf-8"))

    def scrape(self, limit: int = 0) -> None:
        fetched_max_id = (
            self.__db.fetch_val("select tid from thread order by tid desc limit 1") or 1
        )

        c = 0

        for ids in itertools.batched(range(fetched_max_id, known_max_id), 100):
            current_ids = {
                x[0]
                for x in self.__db.fetch_all(
                    """select tid from thread where tid = any($1)""", [ids]
                )
            }

            missing = [x for x in ids if x not in current_ids]

            for i in missing:
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

                c += 1
                if limit and c >= limit:
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
            tc = self.mteam_client.download_torrent(tid=tid)

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

    def __run(self) -> None:
        limit = parse_obj_as(int, os.environ.get("SCRAPE_LIMIT", "100"))
        while True:
            logger.info("fetch torrents")
            try:
                no_pending = self.fetch_torrent()
            except httpx_network_errors:
                time.sleep(60)
                continue
            except MTeamRequestError as e:
                if e.message == "請求過於頻繁":
                    logger.info("operator {!r} get rate limited, sleep for 1h", e.op)
                    time.sleep(3600)
                else:
                    logger.exception("failed to fetch torrents")
                    time.sleep(60)
                continue

            if not no_pending:
                time.sleep(10)
                continue

            # zero pending torrents to download, scrape new threads
            try:
                self.scrape(limit=limit)
            except httpx_network_errors:
                time.sleep(60)
                continue
            except MTeamRequestError as e:
                if e.message == "請求過於頻繁":
                    logger.info("operator {!r} get rate limited, sleep for 1h", e.op)
                    time.sleep(3600)
                else:
                    logger.exception("failed to fetch threads")
                    time.sleep(60)
                continue

            time.sleep(60)

    def start(self) -> None:
        self.__run()
