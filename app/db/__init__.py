import contextlib
from collections.abc import Iterator, Sequence
from typing import Any, LiteralString

import psycopg.connection
from psycopg import RawCursor
from psycopg_pool import ConnectionPool

from . import dlock


class Connection(psycopg.connection.Connection):
    def fetch_val(
        self,
        sql: LiteralString,
        args: Sequence[Any] = (),
    ) -> Any:
        row = self.fetch_one(sql, args)
        if row is None:
            return None
        return row[0]

    def fetch_one(
        self,
        sql: LiteralString,
        args: Sequence[Any] = (),
    ) -> tuple[Any, ...] | None:
        return self.execute(sql, args).fetchone()

    def fetch_all(
        self,
        sql: LiteralString,
        args: Sequence[Any] = (),
    ) -> list[tuple[Any, ...]]:
        return self.execute(sql, args).fetchall()


class Database:
    def __init__(self, dsn: str):
        self.__conn_info = dsn

        self.db = ConnectionPool(
            self.__conn_info,
            kwargs={"cursor_factory": RawCursor},
            max_size=3,
            min_size=1,
            connection_class=Connection,
        )

    def connection(self) -> contextlib.AbstractContextManager[Connection]:
        return self.db.connection()

    def lock(self, key: str) -> dlock.Lock:
        return dlock.Lock(self.__conn_info, key, scope="session")

    def execute(self, sql: LiteralString, args: Sequence[Any] = ()) -> None:
        with self.db.connection() as conn:
            conn.execute(sql, args)

    def fetch_val(self, sql: LiteralString, args: Sequence[Any] = ()) -> Any:
        with self.connection() as conn:
            return conn.fetch_val(sql, args)

    def fetch_one(
        self,
        sql: LiteralString,
        args: Sequence[Any] = (),
    ) -> tuple[Any, ...] | None:
        with self.connection() as conn:
            return conn.fetch_one(sql, args)

    def fetch_all(
        self,
        sql: LiteralString,
        args: Sequence[Any] = (),
    ) -> list[tuple[Any, ...]]:
        with self.connection() as conn:
            return conn.fetch_all(sql, args)

    def stream(
        self,
        sql: LiteralString,
        args: Sequence[Any] = (),
    ) -> Iterator[tuple[Any, ...]]:
        with self.connection() as conn, conn.cursor() as cursor:
            yield from cursor.stream(sql, args)
