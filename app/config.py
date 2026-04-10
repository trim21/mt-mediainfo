import dataclasses
import os
import uuid
from typing import Annotated, Any

import durationpy
import yarl
from pydantic import BeforeValidator, ByteSize, Field, HttpUrl

from app.utils import parse_obj


def parse_go_duration_str(s: Any) -> Any:
    if isinstance(s, float | int):
        return int(s)

    if isinstance(s, str):
        return int(durationpy.from_str(s).total_seconds())

    return s


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    debug: Annotated[bool, Field(os.getenv("DEBUG") or False, validate_default=False)]
    node_id: Annotated[
        uuid.UUID,
        Field(
            alias="node-id",
            min_length=1,
            default_factory=lambda: os.getenv("NODE_ID") or uuid.UUID(int=uuid.getnode()),
        ),
    ]

    mt_token: Annotated[
        str, Field(min_length=1, default_factory=lambda: os.environ["MT_API_TOKEN"])
    ]

    # filter empty string
    http_proxy: Annotated[str | None, BeforeValidator(lambda x: x or None)] = None

    pg_host: Annotated[str, Field(os.environ.get("PG_HOST", "127.0.0.1"), validate_default=True)]
    pg_port: Annotated[int, Field(os.environ.get("PG_PORT", "5432"), validate_default=True)]
    pg_db: Annotated[str, Field(os.environ.get("PG_DB", "postgres"))]
    pg_user: Annotated[
        str | None, Field(os.environ.get("PG_USER", "postgres"), validate_default=True)
    ]
    pg_password: Annotated[
        str | None, None, Field(os.environ.get("PG_PASSWORD", "postgres"), validate_default=True)
    ]

    pg_sslmode: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(os.environ.get("PG_SSLMODE")),
    ]
    pg_ssl_rootcert: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(os.environ.get("PG_SSL_ROOTCERT")),
    ]
    pg_ssl_cert: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(os.environ.get("PG_SSL_CERT")),
    ]
    pg_ssl_key: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(os.environ.get("PG_SSL_KEY")),
    ]

    qb_url: Annotated[
        HttpUrl, Field(os.environ.get("QB_URL", "http://127.0.0.1:8084"), validate_default=True)
    ]

    download_path: Annotated[
        str,
        Field(
            os.environ.get("DOWNLOAD_PATH", os.path.expanduser("~/downloads")),
            validate_default=True,
        ),
    ]

    total_process_size: Annotated[
        ByteSize,
        Field(os.environ.get("TOTAL_SIZE", "100GiB"), validate_default=True),
    ]

    single_torrent_size_limit: Annotated[
        ByteSize,
        Field(os.environ.get("SINGLE_TORRENT_SIZE_LIMIT", "10GiB"), validate_default=True),
    ]

    def pg_dsn(self) -> str:
        url = yarl.URL.build(
            scheme="postgresql",
            user=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port,
            path="/" + self.pg_db,
        )

        query: dict[str, str] = {}
        if self.pg_sslmode:
            query["sslmode"] = self.pg_sslmode
        if self.pg_ssl_rootcert:
            query["sslrootcert"] = self.pg_ssl_rootcert
        if self.pg_ssl_cert:
            query["sslcert"] = self.pg_ssl_cert
        if self.pg_ssl_key:
            os.chmod(self.pg_ssl_key, 0o600)
            query["sslkey"] = self.pg_ssl_key

        if query:
            url = url.with_query(query)

        return str(url)


def load_config() -> Config:
    return parse_obj(Config, {})
