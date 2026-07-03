from datetime import datetime, timedelta

from .database import Database


class KVConfig:
    def __init__(self, db: Database):
        self.__db = db

    def get(self, key: str, default: str | None = None) -> str | None:
        val = self.__db.fetch_val(
            """
                select value from config
                    where key = $1 and (expires_at is null or expires_at > CURRENT_TIMESTAMP)
                """,
            [key],
        )
        if val is not None:
            return val
        return default

    def set(self, key: str, value: str, ttl: timedelta | None = None) -> None:
        expires_at = datetime.now().astimezone() + ttl if ttl else None
        self.__db.execute(
            """
            insert into config (key, value, expires_at) values ($1, $2, $3)
            on conflict (key) do update set value = excluded.value, expires_at = excluded.expires_at
            """,
            [key, value, expires_at],
        )

    def inc(self, key: str, ttl: timedelta | None = None) -> int:
        expires_at = datetime.now().astimezone() + ttl if ttl else None
        with self.__db.connection() as conn:
            return conn.fetch_val(
                """
                insert into config (key, value, expires_at) values ($1, '1', $2)
                on conflict (key) do update set
                    value = config.value::int + 1,
                    expires_at = coalesce(excluded.expires_at, config.expires_at)
                returning value::int
                """,
                [key, expires_at],
            )

    def delete(self, key: str) -> None:
        self.__db.execute("delete from config where key = $1", [key])

    def cleanup(self) -> int:
        """Delete expired config entries. Returns number of rows deleted."""
        with self.__db.connection() as conn:
            cur = conn.execute(
                """
                delete from config where expires_at is not null and expires_at <= CURRENT_TIMESTAMP
                """
            )
            return cur.rowcount
