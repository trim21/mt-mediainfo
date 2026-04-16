import dataclasses
import os
import stat
import tempfile
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


def default_node_id() -> str:
    return os.getenv("NODE_ID") or str(uuid.UUID(int=uuid.getnode()))


@dataclasses.dataclass(frozen=True, kw_only=True)
class BaseConfig:
    debug: Annotated[bool, Field(os.getenv("DEBUG") or False, validate_default=False)]

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
            query["sslkey"] = _copy_key_with_permissions(self.pg_ssl_key)

        if query:
            url = url.with_query(query)

        return str(url)


@dataclasses.dataclass(kw_only=True, frozen=True)
class S3Mixin:
    s3_bucket: Annotated[
        str,
        Field(min_length=1, default_factory=lambda: os.environ["S3_BUCKET"]),
    ]
    s3_region: Annotated[
        str,
        Field(min_length=1, default_factory=lambda: os.environ["S3_REGION"]),
    ]
    s3_endpoint: Annotated[
        str,
        Field(min_length=1, default_factory=lambda: os.environ["S3_ENDPOINT"]),
    ]
    s3_access_key_id: Annotated[
        str,
        Field(min_length=1, default_factory=lambda: os.environ["S3_ACCESS_KEY_ID"]),
    ]
    s3_secret_access_key: Annotated[
        str,
        Field(min_length=1, default_factory=lambda: os.environ["S3_SECRET_ACCESS_KEY"]),
    ]
    s3_root: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(os.environ.get("S3_ROOT")),
    ]


@dataclasses.dataclass(frozen=True, kw_only=True)
class NodeConfig(BaseConfig, S3Mixin):
    node_id: Annotated[
        str,
        Field(
            alias="node-id",
            min_length=1,
            default_factory=default_node_id,
        ),
    ]

    qb_url: Annotated[
        HttpUrl | None,
        BeforeValidator(lambda x: x or None),
        Field(os.environ.get("QB_URL"), validate_default=True),
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


@dataclasses.dataclass(frozen=True, kw_only=True)
class ScrapeConfig(BaseConfig, S3Mixin):
    mt_token: Annotated[
        str, Field(min_length=1, default_factory=lambda: os.environ["MT_API_TOKEN"])
    ]

    # filter empty string
    http_proxy: Annotated[str | None, BeforeValidator(lambda x: x or None)] = None


@dataclasses.dataclass(frozen=True, kw_only=True)
class ServerConfig(BaseConfig):
    pass


def load_node_config() -> NodeConfig:
    return parse_obj(NodeConfig, {})


def load_scrape_config() -> ScrapeConfig:
    return parse_obj(ScrapeConfig, {})


def load_server_config() -> ServerConfig:
    return parse_obj(ServerConfig, {})


def _copy_key_with_permissions(src: str) -> str:
    """Copy a private key file to a temp file with 0600 permissions.

    Docker volume mounts may not allow chmod on the original file,
    so we copy it to a temp location where we control permissions.
    """
    dst = os.path.join(tempfile.gettempdir(), "pg-client.key")
    # Remove first so os.open O_CREAT applies the mode to a fresh inode
    try:
        os.unlink(dst)
    except FileNotFoundError:
        pass

    fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IWUSR)
    try:
        with open(src, "rb") as src_f:
            os.write(fd, src_f.read())
    finally:
        os.close(fd)

    return dst
