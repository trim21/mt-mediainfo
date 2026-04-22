import dataclasses
from pathlib import Path
from typing import LiteralString, cast

from app.db import Database

_MIGRATIONS_DIR = Path(__file__, "../sql/migrations").resolve()


@dataclasses.dataclass(frozen=True, slots=True)
class Migration:
    version: int
    sql: str


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


def get_expected_schema_version() -> int:
    migrations = _load_migrations()
    if not migrations:
        return 0
    return max(m.version for m in migrations)


def run_migrations(db: Database) -> None:
    db.execute("create table if not exists config (key text primary key, value text not null)")

    row = db.fetch_val("select value from config where key = 'schema_version'")

    migrations = _load_migrations()

    current = int(row) if row is not None else 0
    for m in migrations:
        if m.version <= current:
            continue
        print(f"running migration {m.version}")
        db.execute(cast(LiteralString, m.sql))  # type: ignore[redundant-cast]
        db.execute(
            "insert into config (key, value) values ('schema_version', $1)"
            " on conflict (key) do update set value = excluded.value",
            [str(m.version)],
        )
