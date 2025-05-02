from pathlib import Path

from more_itertools import chunked
from sslog import logger

from app.config import Config
from app.db import Database
from app.mt import MTeamAPI, MTeamRequestError
from app.torrent import parse_torrent
from app.utils import get_info_hash_v1_from_content

known_max_id = 950595


class Scrape:
    mteam_client: MTeamAPI
    __db: Database

    def __init__(self, c: Config):
        self.__db = Database(c)
        self.mteam_client = MTeamAPI(c)

        for sql_file in Path(__file__, "../sql/").resolve().iterdir():
            print("executing {}".format(sql_file.name))
            self.__db.execute(sql_file.read_text(encoding="utf-8"))

    def scrape(self, limit: int = 0) -> None:
        fetched_max_id = (
            self.__db.fetch_val("select tid from thread order by tid desc limit 1") or 1
        )

        c = 0

        for ids in chunked(range(fetched_max_id, known_max_id), 100):
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

                size = r.size
                info_hash = ""
                if not r.status.seeders:
                    tc = self.mteam_client.download_torrent(tid=i)

                    t = parse_torrent(tc)
                    info_hash = get_info_hash_v1_from_content(tc)

                    size = t.total_length

                    self.__db.execute(
                        """
                        insert into torrent (tid, info_hash, content)
                        VALUES ($1, $2, $3)
                    """,
                        [i, info_hash, tc],
                    )

                self.__db.execute(
                    """
                    insert into thread (tid, size, mediainfo, info_hash, category, seeders, deleted)
                    values ($1, $2, $3, $4, $5, $6, false)
                    on conflict (tid) do update set
                    size = excluded.size,
                    mediainfo = excluded.category,
                    category = excluded.category,
                    info_hash = excluded.info_hash,
                    deleted = false
                    """,
                    [
                        i,
                        size,
                        r.mediainfo or "",
                        info_hash,
                        r.category,
                        r.status.seeders,
                    ],
                )

                c += 1
                if limit:
                    if c >= limit:
                        return
