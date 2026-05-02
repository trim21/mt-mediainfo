from __future__ import annotations

from datetime import datetime, timedelta

from app.db import Database


class KVConfig:
    def __init__(self, db: Database):
        self.__db = db

    def get(self, key: str, default: str | None = None) -> str | None:
        val = self.__db.fetch_val(
            "select value from config where key = $1 and (expires_at is null or expires_at > now())",
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

    def cleanup(self) -> int:
        """Delete expired config entries. Returns number of rows deleted."""
        with self.__db.connection() as conn:
            cur = conn.execute(
                "delete from config where expires_at is not null and expires_at <= CURRENT_TIMESTAMP"
            )
            return cur.rowcount
