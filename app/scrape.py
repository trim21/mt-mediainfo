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

                tc = self.mteam_client.download_torrent(tid=i)

                t = parse_torrent(tc)

                self.__db.execute(
                    """
                    insert into torrent (tid, info_hash, content)
                    VALUES ($1, $2, $3)
                """,
                    [i, get_info_hash_v1_from_content(tc), tc],
                )

                self.__db.execute(
                    """
                    insert into thread (tid, size, mediainfo, category, deleted)
                    values ($1, $2, $3, $4, false)
                    on conflict (tid) do update set
                    size = excluded.size,
                    mediainfo = excluded.category,
                    category = excluded.category,
                    deleted = false
                    """,
                    [i, t.total_length, r.mediainfo or "", r.category],
                )

                c += 1
                if limit:
                    if c >= limit:
                        return
