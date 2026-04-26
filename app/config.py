import dataclasses
import os
import stat
import tempfile
import uuid
from pathlib import Path
from typing import Annotated, Any

import durationpy
import yarl
from pydantic import BeforeValidator, ByteSize, Field, HttpUrl

from app.const import PickStrategy, SeederFilter
from app.utils import parse_obj


def parse_go_duration_str(s: Any) -> Any:
    if isinstance(s, float | int):
        return int(s)

    if isinstance(s, str):
        return int(durationpy.from_str(s).total_seconds())

    return s


def _data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", os.path.expanduser("~/.local/share/mt-mediainfo")))


def default_node_id() -> str:
    data_dir = _data_dir()
    node_id_file = data_dir.joinpath("node_id")

    if node_id_file.exists():
        return node_id_file.read_text().strip()

    node_id = str(uuid.uuid4())
    os.makedirs(data_dir, exist_ok=True)
    node_id_file.write_text(node_id)
    return node_id


@dataclasses.dataclass(frozen=True, kw_only=True)
class BaseConfig:
    debug: Annotated[bool, Field(alias="DEBUG", default=False, validate_default=False)]

    pg_host: Annotated[str, Field(alias="PG_HOST", default="127.0.0.1", validate_default=True)]
    pg_port: Annotated[int, Field(alias="PG_PORT", default="5432", validate_default=True)]
    pg_db: Annotated[str, Field(alias="PG_DB", default="postgres")]
    pg_user: Annotated[
        str | None, Field(alias="PG_USER", default="postgres", validate_default=True)
    ]
    pg_password: Annotated[
        str | None, None, Field(alias="PG_PASSWORD", default="postgres", validate_default=True)
    ]

    pg_sslmode: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="PG_SSLMODE", default=None),
    ]
    pg_ssl_rootcert: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="PG_SSL_ROOTCERT", default=None),
    ]
    pg_ssl_cert: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="PG_SSL_CERT", default=None),
    ]
    pg_ssl_key: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="PG_SSL_KEY", default=None),
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
        Field(alias="S3_BUCKET", min_length=1),
    ]
    s3_region: Annotated[
        str,
        Field(alias="S3_REGION", min_length=1),
    ]
    s3_endpoint: Annotated[
        str,
        Field(alias="S3_ENDPOINT", min_length=1),
    ]
    s3_access_key_id: Annotated[
        str,
        Field(alias="S3_ACCESS_KEY_ID", min_length=1),
    ]
    s3_secret_access_key: Annotated[
        str,
        Field(alias="S3_SECRET_ACCESS_KEY", min_length=1),
    ]
    s3_root: Annotated[
        str | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="S3_ROOT", default=None),
    ]


@dataclasses.dataclass(frozen=True, kw_only=True)
class DownloaderConfig(BaseConfig, S3Mixin):
    node_id: Annotated[
        str,
        Field(
            alias="NODE_ID",
            min_length=1,
            default_factory=default_node_id,
        ),
    ]

    qb_url: Annotated[
        HttpUrl | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="QB_URL", default=None, validate_default=True),
    ]

    download_path: Annotated[
        str,
        Field(
            alias="DOWNLOAD_PATH",
            default=os.path.expanduser("~/downloads"),
            validate_default=True,
        ),
    ]

    total_process_size: Annotated[
        ByteSize,
        Field(alias="TOTAL_SIZE", default="100GiB", validate_default=True),
    ]

    single_torrent_size_limit: Annotated[
        ByteSize,
        Field(alias="SINGLE_TORRENT_SIZE_LIMIT", default="10GiB", validate_default=True),
    ]

    pick_strategy: Annotated[
        PickStrategy,
        Field(alias="PICK_STRATEGY", default=PickStrategy.seeders, validate_default=True),
    ]

    seeder_filter: Annotated[
        SeederFilter | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="SEEDER_FILTER", default=None),
    ]

    seeder_threshold: Annotated[
        int | None,
        BeforeValidator(lambda x: x or None),
        Field(alias="SEEDER_THRESHOLD", default=None),
    ]

    version: Annotated[
        str,
        Field(alias="APP_VERSION", default="", validate_default=True),
    ]


@dataclasses.dataclass(frozen=True, kw_only=True)
class ScrapeConfig(BaseConfig, S3Mixin):
    mt_token: Annotated[str, Field(alias="MT_API_TOKEN", min_length=1)]

    # filter empty string
    http_proxy: Annotated[str | None, BeforeValidator(lambda x: x or None)] = None


@dataclasses.dataclass(frozen=True, kw_only=True)
class ServerConfig(BaseConfig):
    pass


@dataclasses.dataclass(frozen=True, kw_only=True)
class S3Config(S3Mixin):
    pass


def load_s3_config() -> S3Config:
    return parse_obj(S3Config, dict(os.environ))


def load_downloader_config() -> DownloaderConfig:
    return parse_obj(DownloaderConfig, dict(os.environ))


def load_scrape_config() -> ScrapeConfig:
    return parse_obj(ScrapeConfig, dict(os.environ))


def load_server_config() -> ServerConfig:
    return parse_obj(ServerConfig, dict(os.environ))


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
