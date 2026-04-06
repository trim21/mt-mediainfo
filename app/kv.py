from app.db import Database


class KVConfig:
    def __init__(self, db: Database):
        self.__db = db

    def get(self, key: str, default: str | None = None) -> str | None:
        val = self.__db.fetch_val("select value from config where key = $1", [key])
        if val is not None:
            return val
        return default

    def set(self, key: str, value: str) -> None:
        self.__db.execute(
            """
            insert into config (key, value) values ($1, $2)
            on conflict (key) do update set value = excluded.value
            """,
            [key, value],
        )
