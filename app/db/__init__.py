import contextlib
import dataclasses
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, LiteralString, Self, cast

import psycopg.connection
from pg_dlock import Lock, Locker
from psycopg import RawCursor
from psycopg_pool import ConnectionPool
from sslog import logger

_MIGRATIONS_DIR = Path(__file__, "../../sql/migrations").resolve()


@dataclasses.dataclass(frozen=True, slots=True)
class Migration:
    version: int
    sql: str


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
        self.__locker = Locker(dsn)

        self.db = ConnectionPool(
            self.__conn_info,
            kwargs={"cursor_factory": RawCursor},
            max_size=3,
            min_size=1,
            connection_class=Connection,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.db.close()

    def connection(self) -> contextlib.AbstractContextManager[Connection]:
        return self.db.connection()

    def lock(self, key: str) -> Lock:
        return self.__locker.lock(key, scope="session")

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

    def run_migrations(self) -> None:
        self.execute(
            "create table if not exists config (key text primary key, value text not null)"
        )

        row = self.fetch_val("select value from config where key = 'schema_version'")

        migrations = _load_migrations()

        current = int(row) if row is not None else 0
        for m in migrations:
            if m.version <= current:
                continue
            print(f"running migration {m.version}")
            self.execute(cast(LiteralString, m.sql))  # type: ignore[redundant-cast]
            self.execute(
                "insert into config (key, value) values ('schema_version', $1)"
                " on conflict (key) do update set value = excluded.value",
                [str(m.version)],
            )

    def wait_db_migration(self) -> None:
        expected = _expected_schema_version()
        while True:
            row = self.fetch_val("select value from config where key = 'schema_version'")
            current = int(row) if row is not None else 0
            if current >= expected:
                break
            logger.warning(
                "schema version mismatch: expected {}, got {}. sleeping 5s...",
                expected,
                current,
            )
            time.sleep(5)


def _load_migrations() -> list[Migration]:
    migrations: list[Migration] = []
    if _MIGRATIONS_DIR.exists():
        for f in sorted(_MIGRATIONS_DIR.iterdir()):
            if f.is_file() and f.suffix == ".sql":
                migrations.append(
                    Migration(
                        version=int(f.stem.split("_")[0]),
                        sql=f.read_text(encoding="utf-8"),
                    )
                )
    return migrations


def _expected_schema_version() -> int:
    migrations = _load_migrations()
    if not migrations:
        return 0
    return max(m.version for m in migrations)
