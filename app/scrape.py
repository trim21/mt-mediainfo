from loguru import logger
from more_itertools import chunked

from app.config import Config, load_config
from app.db import Database
from app.mt import MTeamAPI, MTeamRequestError

known_max_id = 950595


class Scrape:
    mteam_client: MTeamAPI
    __db: Database

    def __init__(self, c: Config):
        self.__db = Database(c)
        self.mteam_client = MTeamAPI(c)

    def scrape(self):
        for ids in chunked(range(1, known_max_id), 100):
            current_ids = {
                x[0]
                for x in self.__db.fetch_all(
                    """select tid from torrent where tid = any($1)""", [ids]
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
                        insert into torrent (tid, deleted)
                        values ($1, true)
                        on conflict (tid) do update set deleted = true
                        """,
                            [i],
                        )
                        continue
                    raise

                self.__db.execute(
                    """
                    insert into torrent (tid, size, mediainfo, category, deleted)
                    values ($1, $2, $3, $4, false)
                    on conflict (tid) do update set
                    size = excluded.size,
                    mediainfo = excluded.category,
                    category = excluded.category,
                    deleted = false
                    """,
                    [i, r.size, r.mediainfo or "", r.category],
                )


if __name__ == "__main__":
    Scrape(load_config()).scrape()
