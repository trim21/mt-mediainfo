import contextlib
import dataclasses
import enum
import io
import os.path
import shutil
import sqlite3
import sys
import time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString, NamedTuple, cast

import jinja2
import psycopg
import qbittorrentapi
from psycopg.rows import dict_row
from rich.console import Console
from rtorrent_rpc import RTorrent
from sslog import logger

from app.bt_client import (
    ETA_INF,
    BTClient,
    Torrent,
    TorrentFile,
    TorrentNotFoundError,
    TorrentState,
)
from app.bt_client.neptune_client import NeptuneClient
from app.bt_client.qb_client import QBittorrentClient
from app.bt_client.rt_client import RTorrentClient
from app.config import DownloaderConfig
from app.const import (
    BT_TAG_DOWNLOADING,
    BT_TAG_FILE_SELECTED,
    BT_TAG_PROCESS_ERROR,
    BT_TAG_PROCESSING,
    BT_TAG_SELECTING_FILES,
    EXCLUDED_CATEGORY,
    TZ_SHANGHAI,
    ItemStatus,
    pick_order_clause,
)
from app.db import Connection, Database
from app.hardcode_subtitle import check_hardcode_chinese_subtitle
from app.mediainfo import extract_bdinfo_from_dir, extract_mediainfo_from_file
from app.mt import MTeamDomain
from app.rpc import (
    RPC_DELETE_TORRENT,
    RPC_PING,
    DeleteTorrentPayload,
    PingPayload,
    process_commands,
)
from app.torrent import (
    File,
    bdmv_disc_path,
)
from app.torrent_store import TorrentStore
from app.utils import human_readable_size, must_find_executable, set_torrent_comment


def format_exc(e: Exception) -> str:
    f = io.StringIO()
    with f:
        f.write(f"{e}\n")
        Console(legacy_windows=True, width=1000, file=f, no_color=True).print_exception()
        return f.getvalue()


class Status(enum.IntEnum):
    unknown = 0
    downloading = 1
    done = 3


@dataclasses.dataclass(frozen=True, slots=True)
class PickContext:
    picked: int = 0
    has_pending: bool = False
    no_space: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class LoopContext:
    min_eta: float = 300


class JobMeta(NamedTuple):
    tid: int
    is_bdmv: bool
    selected_index: list[int]


def _init_progress_db(data_dir: Path) -> Path:
    """Create and migrate the local SQLite progress database. Returns the db path."""
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "progress.db"
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        _run_progress_migrations(conn)
    return db_path


def _pick_query(config: DownloaderConfig) -> LiteralString:
    # Inline the pending_download_threads view predicates instead of selecting from
    # the view, because FOR UPDATE cannot be used on views.  The query locks
    # candidate rows on the thread table directly so that concurrent downloaders
    # pick disjoint sets without a global advisory lock — PostgreSQL's row-level
    # SKIP LOCKED handles the coordination natively.  Columns are listed
    # explicitly to avoid transferring the large mediainfo/api_mediainfo text
    # columns over the wire.
    order_clause = pick_order_clause(config.pick_strategy)

    seeder_clause: LiteralString = cast(LiteralString, config.seeder_condition)

    return f"""
    select
        thread.tid, thread.size, thread.info_hash, thread.seeders,
        thread.category, thread.deleted, thread.created_at, thread.upload_at,
        thread.api_mediainfo_at, thread.torrent_fetched_at, thread.selected_size,
        thread.torrent_invalid, thread.generated_mediainfo_at, thread.exported_at,
        thread.selected_index, thread.type
    from thread
    where
        thread.deleted = false
        and thread.seeders != 0
        and thread.mediainfo = ''
        and thread.api_mediainfo = ''
        and thread.info_hash != ''
        and thread.selected_size > 0
        and thread.selected_size < $1
        and not (thread.category = any ($2))
        and thread.selected_index is not null
        and array_length(thread.selected_index, 1) > 0
        and ({seeder_clause})
        and not exists (select 1 from job where job.tid = thread.tid)
    {order_clause}
    limit $3
    for update of thread skip locked
    """


_PROGRESS_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "sql" / "progress_migrations"


def _run_progress_migrations(db: sqlite3.Connection) -> None:
    """Run SQLite migrations, tracking applied versions in `schema_version` table."""
    if not _PROGRESS_MIGRATIONS_DIR.exists():
        return
    db.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
    current = db.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
    for f in sorted(_PROGRESS_MIGRATIONS_DIR.iterdir()):
        if not (f.is_file() and f.suffix == ".sql"):
            continue
        version = int(f.stem.split("_")[0])
        if version <= current:
            continue
        db.executescript(f.read_text(encoding="utf-8"))
        db.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))
        db.commit()


@dataclasses.dataclass(kw_only=True, frozen=True)
class Downloader:
    db: Database
    config: DownloaderConfig
    client: BTClient
    store: TorrentStore
    _progress_db_path: Path
    _last_orphan_cleanup: list[float] = dataclasses.field(default_factory=lambda: [0.0])
    mediainfo_bin: str = dataclasses.field(
        default_factory=lambda: must_find_executable("mediainfo")
    )
    ffprobe_bin: str = dataclasses.field(default_factory=lambda: must_find_executable("ffprobe"))
    ffmpeg_bin: str = dataclasses.field(default_factory=lambda: must_find_executable("ffmpeg"))
    bdinfo_bin: str = dataclasses.field(default_factory=lambda: must_find_executable("BDInfo"))
    thread_filter_template: jinja2.Template | None = None
    _mediainfo_cache: dict[str, tuple[str, bool]] = dataclasses.field(default_factory=dict)

    @classmethod
    def new(cls, cfg: DownloaderConfig) -> Downloader:
        logger.info("initializing downloader {}, node_id={}", cfg.version, cfg.node_id)
        logger.info("connecting to database...")
        db = Database(cfg.pg_dsn())
        logger.info("database pool created")
        if cfg.neptune_url and cfg.neptune_token:
            client: BTClient = NeptuneClient(
                base_url=cfg.neptune_url,
                token=cfg.neptune_token,
            )
        elif cfg.rt_url:
            client = RTorrentClient(
                RTorrent(cfg.rt_url, timeout=cfg.rt_timeout),
            )
        elif cfg.qb_url:
            client = QBittorrentClient(
                qbittorrentapi.Client(
                    host=str(cfg.qb_url),
                    password=cfg.qb_url.password,
                    username=cfg.qb_url.username,
                    SIMPLE_RESPONSES=True,
                    FORCE_SCHEME_FROM_HOST=True,
                    VERBOSE_RESPONSE_LOGGING=False,
                    RAISE_NOTIMPLEMENTEDERROR_FOR_UNIMPLEMENTED_API_ENDPOINTS=True,
                    REQUESTS_ARGS={"timeout": 10},
                ),
            )
        else:
            raise ValueError(
                "no download client configured: set NEPTUNE_URL+NEPTUNE_TOKEN, RT_URL or QB_URL"
            )

        logger.info("download client created, type={}", type(client).__name__)

        thread_filter_template: jinja2.Template | None = None
        if cfg.thread_filter:
            thread_filter_template = jinja2.Environment().from_string(cfg.thread_filter)

        store = TorrentStore(cfg)
        logger.info("torrent store created")

        progress_db_path = _init_progress_db(cfg.data_dir)
        logger.info("local progress database initialized")

        return Downloader(
            config=cfg,
            db=db,
            client=client,
            store=store,
            thread_filter_template=thread_filter_template,
            _progress_db_path=progress_db_path,
        )

    def __post_init__(self) -> None:
        logger.info("testing database connection...")
        try:
            self.db.fetch_val("select version()")
        except Exception:
            logger.exception("failed to connect to database")
            sys.exit(1)

        logger.info("successfully connect to database")

        logger.info("waiting for database migrations...")
        self.db.wait_db_migration()

        logger.info("connecting to download client...")
        version = self.client.app_version()
        logger.info("successfully connect to download client {}", version)

        logger.info("using mediainfo at {}", self.mediainfo_bin)
        logger.info("using ffprobe at {}", self.ffprobe_bin)
        logger.info("using ffmpeg at {}", self.ffmpeg_bin)
        logger.info("using bdinfo at {}", self.bdinfo_bin)

    def _progress_record(self, info_hash: str, size: int) -> bool:
        """Record a download progress sample. Returns True if a new sample was inserted."""
        with closing(sqlite3.connect(str(self._progress_db_path))) as conn:
            last = conn.execute(
                "SELECT size FROM progress WHERE info_hash = ? ORDER BY recorded_at DESC LIMIT 1",
                (info_hash,),
            ).fetchone()
            if last is not None and last[0] == size:
                return False
            conn.execute(
                "INSERT INTO progress (info_hash, size, recorded_at) VALUES (?, ?, ?)",
                (info_hash, size, time.time()),
            )
            conn.commit()
            return True

    def _progress_forget(self, info_hash: str) -> None:
        """Remove all progress records for a torrent (job completed / failed / evicted)."""
        with closing(sqlite3.connect(str(self._progress_db_path))) as conn:
            conn.execute("DELETE FROM progress WHERE info_hash = ?", (info_hash,))
            conn.commit()

    def _progress_cleanup(self, active_hashes: set[str]) -> None:
        """Remove progress records for hashes that are no longer actively downloading."""
        with closing(sqlite3.connect(str(self._progress_db_path))) as conn:
            if not active_hashes:
                conn.execute("DELETE FROM progress")
            else:
                placeholders = ",".join("?" for _ in active_hashes)
                conn.execute(
                    f"DELETE FROM progress WHERE info_hash NOT IN ({placeholders})",
                    tuple(active_hashes),
                )
            conn.commit()

    def _progress_stalled(self, active_hashes: set[str], cutoff: float) -> set[str]:
        """Return info_hashes whose last recorded progress is before `cutoff` (Unix timestamp)."""
        if not active_hashes:
            return set()
        with closing(sqlite3.connect(str(self._progress_db_path))) as conn:
            rows = conn.execute(
                "SELECT info_hash FROM progress GROUP BY info_hash HAVING MAX(recorded_at) < ?",
                (cutoff,),
            ).fetchall()
            return {r[0] for r in rows}

    def _progress_avg_speed(self, info_hash: str, window: float = 1800) -> float | None:
        """Return average download speed (bytes/s) over the last `window` seconds,
        or None if there are fewer than 2 samples in the window.

        Uses current time as the end of the interval so that idle periods
        (where no new samples are inserted because size hasn't changed)
        are reflected as a decaying average speed."""
        now = time.time()
        with closing(sqlite3.connect(str(self._progress_db_path))) as conn:
            row = conn.execute(
                """
                SELECT (MAX(size) - MIN(size)) * 1.0 / MAX(1, ? - MIN(recorded_at))
                FROM progress
                WHERE info_hash = ? AND recorded_at > ?
                HAVING COUNT(*) >= 2
                """,
                (now, info_hash, now - window),
            ).fetchone()
            return row[0] if row else None

    def _progress_slowest(self, cutoff: float) -> tuple[str, float] | None:
        """Return (info_hash, avg_speed) of the slowest downloading torrent with samples after `cutoff`.

        Uses current time as the end of the interval so that idle periods are reflected
        as a decaying average speed."""
        now = time.time()
        with closing(sqlite3.connect(str(self._progress_db_path))) as conn:
            row = conn.execute(
                """
                SELECT info_hash,
                       (MAX(size) - MIN(size)) * 1.0 / MAX(1, ? - MIN(recorded_at)) AS avg_speed
                FROM progress
                WHERE recorded_at > ?
                GROUP BY info_hash
                HAVING COUNT(*) >= 2
                ORDER BY avg_speed
                LIMIT 1
                """,
                (now, cutoff),
            ).fetchone()
            if row is None:
                return None
            return row[0], row[1]

    def start(self) -> None:
        logger.info("downloader started, entering main loop")
        interval = 1
        while True:
            self.__heart_beat()
            self.__wait_for_notify(interval)
            interval = 60 * 5
            try:
                self.__process_commands()
            except Exception:
                logger.exception("failed to process commands")

            self.client.tick()

            try:
                loop_ctx = self.__run_at_interval()
            except Exception:
                logger.exception("failed to run")
                loop_ctx = LoopContext()

            interval = min(300, abs(int(loop_ctx.min_eta)))

            self._report_status("waiting")
            try:
                commands_processed = self.__process_commands()
            except Exception:
                logger.exception("failed to process commands")
                commands_processed = False
            if commands_processed:
                logger.info("rpc commands processed, skipping sleep")
                continue
            logger.info("loop done, sleeping for {}s", interval)

    def __wait_for_notify(self, timeout: float) -> None:
        """Wait for a PG notification or until timeout expires."""
        try:
            channel = f"node_rpc_{self.config.node_id}"
            with psycopg.connect(self.config.pg_dsn(), autocommit=True) as conn:
                conn.execute(f'LISTEN "{channel}"')  # type: ignore
                for _ in conn.notifies(timeout=timeout, stop_after=1):
                    pass
        except Exception:
            time.sleep(timeout)

    def __process_commands(self) -> bool:
        """Poll and execute pending RPC commands for this downloader."""
        return process_commands(
            self.db,
            self.config.node_id,
            {
                RPC_DELETE_TORRENT: self.__handle_cmd_delete_torrent,
                RPC_PING: self.__handle_cmd_ping,
            },
        )

    @staticmethod
    def __handle_cmd_ping(_payload: PingPayload) -> dict[str, str]:
        return {"pong": "ok"}

    def _delete_torrent_files(self, info_hash: str) -> None:
        save_path = Path(self.config.download_path) / info_hash.lower()
        shutil.rmtree(save_path, ignore_errors=True)

    def __handle_cmd_delete_torrent(self, payload: DeleteTorrentPayload) -> dict[str, str]:
        self.client.torrents_delete(torrent_hashes=payload.info_hash, delete_files=True)
        self._delete_torrent_files(payload.info_hash)
        self.__update_job_status(
            status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
            info_hash=payload.info_hash,
            removed_reason="rpc",
        )
        return {"info_hash": payload.info_hash}

    def __cleanup_orphan_files(self) -> None:
        """Delete download directories that have no corresponding active job.

        Runs every 3 hours. Scans download_path for info_hash directories
        and removes any that don't have an active job in the database.
        """
        elapsed = time.monotonic() - self._last_orphan_cleanup[0]
        if elapsed < 3 * 3600:
            return

        download_path = Path(self.config.download_path)
        if not download_path.is_dir():
            return

        logger.info("starting orphan file cleanup")

        # Get all info_hashes with active jobs (any status except terminal ones)
        active_hashes: set[str] = {
            row[0]
            for row in self.db.fetch_all(
                """
                select distinct info_hash from job
                where status not in ('done', 'failed', 'removed_from_download_client', 'skipped')
                """
            )
        }

        # Also include hashes with completed jobs (files might still be needed briefly)
        # and hashes currently in the download client
        try:
            client_hashes = {t.hash for t in self.client.torrents_info()}
        except Exception:
            logger.exception("failed to get torrents info for orphan cleanup")
            return

        protected_hashes = active_hashes | client_hashes

        removed_count = 0
        removed_size = 0

        for entry in download_path.iterdir():
            if not entry.is_dir():
                continue

            info_hash = entry.name.lower()
            if info_hash in protected_hashes:
                continue

            try:
                dir_size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
                shutil.rmtree(entry)
                removed_count += 1
                removed_size += dir_size
                logger.info(
                    "removed orphan directory {} ({})", entry.name, human_readable_size(dir_size)
                )
            except Exception:
                logger.exception("failed to remove orphan directory {}", entry.name)

        if removed_count > 0:
            logger.info(
                "orphan cleanup done: removed {} directories, freed {}",
                removed_count,
                human_readable_size(removed_size),
            )
        else:
            logger.info("orphan cleanup done: no orphan files found")

        self._last_orphan_cleanup[0] = time.monotonic()

    def __run_at_interval(self) -> LoopContext:
        with contextlib.suppress(Exception):
            self.__cleanup_orphan_files()

        self._report_status("torrents")
        completed, min_eta = self.__process_torrents()
        self._report_status("picking")
        ctx = self.__pick_and_add_jobs()
        if not completed and ctx.picked == 0 and ctx.no_space and ctx.has_pending:
            self.__maybe_evict_slowest()
        return LoopContext(min_eta=min_eta)

    def _report_status(self, status: str) -> None:
        """Update node status directly in DB for hang-debugging visibility."""
        if self.config.disable_status_report:
            return
        with contextlib.suppress(Exception):
            self.db.execute(
                "update node set status = $1, last_seen = now() where id = $2",
                [status, self.config.node_id],
            )

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen, version) values ($1, $2, $3)
            on conflict (id) do update set last_seen = excluded.last_seen, version = excluded.version
            """,
            [
                self.config.node_id,
                datetime.now(tz=TZ_SHANGHAI),
                self.config.version,
            ],
        )

    def __set_tags(self, info_hash: str, *, remove: str, add: str) -> None:
        """Swap informational tags on a torrent."""
        self.client.torrents_remove_tags(tags=[remove], torrent_hashes=info_hash)
        self.client.torrents_add_tags(tags=[add], torrent_hashes=info_hash)

    @staticmethod
    def __update_job_on_conn(
        conn: Connection,
        node_id: str,
        *,
        status: str,
        info_hash: str = "",
        tid: int = 0,
        failed_reason: str = "",
        removed_reason: str = "",
    ) -> None:
        if info_hash:
            conn.execute(
                """update job set status = $1, failed_reason = $2, removed_reason = $3, updated_at = current_timestamp,
                   completed_at = case when $1 = 'done' then current_timestamp else completed_at end
                   where info_hash = $4 and node_id = $5""",
                [status, failed_reason, removed_reason, info_hash, node_id],
            )
        else:
            conn.execute(
                """update job set status = $1, failed_reason = $2, removed_reason = $3, updated_at = current_timestamp,
                   completed_at = case when $1 = 'done' then current_timestamp else completed_at end
                   where tid = $4 and node_id = $5""",
                [status, failed_reason, removed_reason, tid, node_id],
            )

    def __update_job_status(
        self,
        *,
        status: str,
        info_hash: str = "",
        tid: int = 0,
        failed_reason: str = "",
        removed_reason: str = "",
    ) -> None:
        """Update job status. Identify job by info_hash or tid (plus node_id)."""
        with self.db.connection() as conn, conn.transaction():
            self.__update_job_on_conn(
                conn,
                self.config.node_id,
                status=status,
                info_hash=info_hash,
                tid=tid,
                failed_reason=failed_reason,
                removed_reason=removed_reason,
            )
        if info_hash:
            self._progress_forget(info_hash)

    def __process_torrents(self) -> tuple[bool, float]:
        """Process all torrents in the download client in a single pass.

        Returns (completed, min_eta) where min_eta is the smallest ETA
        among downloading torrents (seconds), or 300 if none.
        """
        t0 = time.monotonic()
        logger.info("__process_torrents")
        torrents = self.client.torrents_info()
        t1 = time.monotonic()
        logger.info("torrents_info: {} torrents in {:.1f}s", len(torrents), t1 - t0)
        now = datetime.now(tz=TZ_SHANGHAI)
        completed = False
        if not torrents:
            logger.info("client has no torrents")
            return False, 300
        min_eta = 300.0
        # Mark jobs as removed-from-client if their torrent is no longer in client
        torrent_hashes = [x.hash for x in torrents]
        removed_rows = self.db.fetch_all(
            """
                update job set
                  status = $1,
                  removed_reason = $6,
                  updated_at = $5
                where (not info_hash = any($2)) and node_id = $3 and status = $4
                returning info_hash
                """,
            [
                ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                torrent_hashes,
                self.config.node_id,
                ItemStatus.DOWNLOADING,
                now,
                "manual",
            ],
        )
        for row in removed_rows:
            self._progress_forget(row[0])

        # Fetch all downloading jobs for this node
        job_rows = self.db.fetch_all(
            """
            select job.info_hash,
                   thread.type,
                   thread.tid,
                   thread.selected_index
            from job
            join thread on (thread.tid = job.tid)
            where job.node_id = $1 and job.status = $2
            """,
            [self.config.node_id, ItemStatus.DOWNLOADING],
        )
        managed_hashes: set[str] = {r[0] for r in job_rows}
        hash_to_meta: dict[str, JobMeta] = {
            r[0].lower(): JobMeta(
                tid=r[2],
                is_bdmv=r[1] == "bdmv",
                selected_index=r[3],
            )
            for r in job_rows
        }

        # Clean up progress records for torrents no longer actively downloading
        self._progress_cleanup(managed_hashes)
        # Stalled detection via local SQLite: no progress for 2+ days
        stale_cutoff = (now - timedelta(days=self.config.stalled_days)).timestamp()
        stalled_hashes = self._progress_stalled(managed_hashes, stale_cutoff)
        t2 = time.monotonic()
        logger.info(
            "db queries done in {:.1f}s, managed={} stalled={}",
            t2 - t1,
            len(managed_hashes),
            len(stalled_hashes),
        )

        counts: dict[str, int] = {}
        downloading_torrents: list[Torrent] = []
        uploading_torrents: list[Torrent] = []

        # ---- Phase 1: collect downloading/uploading, handle other states ----
        for t in torrents:
            # Torrent not in managed (downloading) jobs — check if it has a job at all
            if t.hash not in managed_hashes:
                self.__handle_unmanaged_torrent(t)
                counts["unmanaged"] = counts.get("unmanaged", 0) + 1
                continue

            # Cleanup stalled torrents (no download size recorded for 3+ days)
            if t.hash in stalled_hashes:
                logger.info("cleanup stalled torrent {}", t.name)
                self.__update_job_status(
                    status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                    info_hash=t.hash,
                    removed_reason="stalled",
                )
                self.db.execute(
                    "update thread set torrent_invalid = 'stalled' where info_hash = $1",
                    [t.hash],
                )
                self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
                self._delete_torrent_files(t.hash)
                counts["stalled"] = counts.get("stalled", 0) + 1
                continue

            # Torrent in error state → mark failed and delete
            if t.state == TorrentState.ERRORED:
                logger.info("torrent {} in error state", t.name)
                self.__update_job_status(
                    status=ItemStatus.FAILED,
                    info_hash=t.hash,
                    failed_reason=t.error_message or "torrent error",
                )
                self._delete_torrent_with_retry(t.hash)
                counts["errored"] = counts.get("errored", 0) + 1
                continue

            # Skip torrents that failed processing
            if BT_TAG_PROCESS_ERROR in t.tags:
                counts["process_error"] = counts.get("process_error", 0) + 1
                continue

            # File not yet selected → select files, clear limit
            if BT_TAG_FILE_SELECTED not in t.tags:
                meta = hash_to_meta[t.hash.lower()]
                self.__fix_file_selection(
                    t,
                    meta.selected_index,
                    meta.is_bdmv,
                )
                self.client.torrents_set_download_limit(limit=0, torrent_hashes=t.hash)
                self.client.torrents_add_tags(tags=[BT_TAG_FILE_SELECTED], torrent_hashes=t.hash)
                if t.state == TorrentState.PAUSED:
                    self.client.torrents_resume(torrent_hashes=t.hash)
                counts["need_select"] = counts.get("need_select", 0) + 1
                continue

            # Defer uploading torrents to phase 3 (individual mediainfo processing)
            if t.state == TorrentState.UPLOADING:
                uploading_torrents.append(t)
                continue

            # Paused → resume (don't do this for uploading; already deferred above)
            if t.state == TorrentState.PAUSED:
                logger.info("resuming stopped torrent {} (tags={})", t.name, t.tags)
                self.__set_tags(t.hash, remove=BT_TAG_SELECTING_FILES, add=BT_TAG_DOWNLOADING)
                self.client.torrents_resume(torrent_hashes=t.hash)
                counts["paused"] = counts.get("paused", 0) + 1
                continue

            # Downloading — accumulate for batch update
            counts["downloading"] = counts.get("downloading", 0) + 1
            downloading_torrents.append(t)

        # ---- Phase 2: batch-update all downloading progress via pipeline ----
        if downloading_torrents:
            min_eta = self.__batch_update_downloading(downloading_torrents, now)

        # ---- Phase 3: process completed torrents one by one (mediainfo extraction) ----
        uploading_count = len(uploading_torrents)
        for i, t in enumerate(uploading_torrents, 1):
            completed = True
            self.__set_tags(t.hash, remove=BT_TAG_DOWNLOADING, add=BT_TAG_PROCESSING)
            meta = hash_to_meta[t.hash.lower()]
            self._report_status(f"mediainfo:{i}/{uploading_count}:{meta.tid}:{t.name[:20]}")
            self.__process_completed_torrent(t, meta)
            counts["uploading"] = counts.get("uploading", 0) + 1

        t3 = time.monotonic()
        logger.info(
            "process_torrents done in {:.1f}s (loop={:.1f}s) counts={} min_eta={:.0f}s",
            t3 - t0,
            t3 - t2,
            counts,
            min_eta,
        )
        return completed, min_eta

    def __batch_update_downloading(self, torrents: list[Torrent], now: datetime) -> float:
        """Batch update job progress and track download progress locally.

        Uses psycopg3 pipeline mode to send all N SQL statements in a single
        connection + sync round-trip.

        Returns min_eta (smallest computed ETA in seconds), or 300 if none.
        """
        node_id = self.config.node_id
        status = ItemStatus.DOWNLOADING
        min_eta = 300.0

        with self.db.connection() as conn, conn.pipeline():
            for t in torrents:
                progress_changed = self._progress_record(t.hash, t.completed)
                avg_speed = self._progress_avg_speed(t.hash, window=1800)
                if avg_speed is None:
                    avg_speed = self._progress_avg_speed(t.hash, window=600)
                if avg_speed is None:
                    avg_speed = 0
                # Compute ETA from our calculated average speed
                if avg_speed and avg_speed > 0:
                    eta = int(max(0, t.size - t.completed) / avg_speed)
                else:
                    eta = ETA_INF
                min_eta = min(min_eta, eta)
                conn.execute(
                    "update job set progress=$1, dlspeed=$2, eta=$3,"
                    " error_message=$4, updated_at=$5"
                    + (", last_progress_at=$9" if progress_changed else "")
                    + " where info_hash=$6 and node_id=$7 and status=$8",
                    [
                        t.completed / t.size if t.size > 0 else 0.0,
                        avg_speed,
                        eta,
                        t.error_message,
                        now,
                        t.hash,
                        node_id,
                        status,
                    ]
                    + ([now] if progress_changed else []),
                )
        return min_eta

    def __handle_unmanaged_torrent(self, t: Torrent) -> None:
        """Handle a torrent in the download client that has no active downloading job.

        Try to reclaim if a job exists with removed-by-client status
        (e.g. was prematurely marked due to async add), otherwise delete it.
        """
        with self.db.connection() as conn, conn.transaction():
            restored = conn.fetch_val(
                """
                update job set status = $1, failed_reason = '', updated_at = current_timestamp
                where info_hash = $2 and node_id = $3 and status = $4
                returning info_hash
                """,
                [
                    ItemStatus.DOWNLOADING,
                    t.hash,
                    self.config.node_id,
                    ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                ],
            )
        if restored:
            logger.info("reclaimed job for torrent {}", t.hash)
            return

        logger.info("{} not managed, deleting from client", t.hash)
        self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
        self._delete_torrent_files(t.hash)
        self._progress_forget(t.hash)

    def __fix_file_selection(self, t: Torrent, selected_index: list[int], is_bdmv: bool) -> None:
        """Fix file priorities for torrents using the pre-computed selected_index."""
        self.__fix_file_selection_by_hash(t.hash, selected_index, is_bdmv, t.name)

    def __fix_file_selection_by_hash(
        self, info_hash: str, selected_index: list[int], is_bdmv: bool, name: str = ""
    ) -> None:
        """Fix file priorities using selected_index from the database."""
        files = self.client.torrents_files(torrent_hash=info_hash)
        if len(files) <= 1:
            return

        if is_bdmv:
            if not selected_index:
                logger.warning(
                    "bdmv torrent {} has no selectable disc, skipping file selection",
                    name or info_hash,
                )
                return
            keep = set(selected_index)
            file_ids = [f.index for f in files if f.index not in keep and f.priority != 0]
            if file_ids:
                logger.info("fixing bdmv file selection for torrent {}", name or info_hash)
                self.client.torrents_file_priority(
                    torrent_hash=info_hash,
                    file_ids=file_ids,
                    priority=0,
                )
            return

        if not selected_index:
            logger.warning(
                "torrent {} has no selected file (no video file found), skipping file selection",
                name or info_hash,
            )
            return
        keep_idx = selected_index[0]
        file_ids = [f.index for f in files if f.index != keep_idx and f.priority != 0]
        if file_ids:
            logger.info("fixing file selection for torrent {}", name or info_hash)
        self.client.torrents_file_priority(
            torrent_hash=info_hash,
            file_ids=file_ids,
            priority=0,
        )

    def __process_completed_torrent(self, t: Torrent, meta: JobMeta) -> None:
        """Process a completed torrent: extract mediainfo and update DB status."""
        files = self.client.torrents_files(torrent_hash=t.hash)

        if not meta.selected_index:
            logger.error(
                "torrent {} has empty selected_index, cannot process (no video file selected)",
                t.name,
            )
            self.__update_job_status(
                status=ItemStatus.FAILED,
                info_hash=t.hash,
                failed_reason="no video file selected (selected_index is empty)",
            )
            self.client.torrents_add_tags(tags=[BT_TAG_PROCESS_ERROR], torrent_hashes=t.hash)
            self._delete_torrent_with_retry(t.hash)
            return

        selected_index = meta.selected_index
        is_bdmv = meta.is_bdmv

        cached = self._mediainfo_cache.get(t.hash)
        if cached is not None:
            logger.info("reusing cached mediainfo for torrent {}", t.name)
            media_info, hard_code_subtitle = cached
        else:
            try:
                if is_bdmv:
                    selected_files = [f for f in files if f.index in selected_index]
                    active_objs = [
                        File(length=f.size, path=tuple(f.name.split("/"))) for f in selected_files
                    ]
                    media_info, hard_code_subtitle = self.__extract_bdmv_mediainfo(
                        active_objs, t.save_path
                    )
                else:
                    target = next((f for f in files if f.index == selected_index[0]), None)
                    if target is None:
                        raise Exception(
                            f"selected file index {selected_index[0]} not found in torrent files"
                        )
                    media_info, hard_code_subtitle = self.__extract_regular_mediainfo(
                        target, t.save_path
                    )
            except Exception as e:
                logger.error("failed to process local torrent {}: {}", t.name, e)
                self.__update_job_status(
                    status=ItemStatus.FAILED,
                    info_hash=t.hash,
                    failed_reason=format_exc(e),
                )
                self.client.torrents_add_tags(tags=[BT_TAG_PROCESS_ERROR], torrent_hashes=t.hash)
                self._delete_torrent_with_retry(t.hash)
                return
            self._mediainfo_cache[t.hash] = (media_info, hard_code_subtitle)

        with (
            self.db.connection() as conn,
            conn.transaction(),
        ):
            conn.execute(
                """
                    update thread set mediainfo = $1, generated_mediainfo_at = current_timestamp, hard_coded_subtitle = $2 where info_hash = $3
                    """,
                [
                    media_info.replace("\x00", ""),
                    hard_code_subtitle,
                    t.hash,
                ],
            )
            conn.execute(
                """update job set status = $1, failed_reason = '', updated_at = current_timestamp,
                   completed_at = current_timestamp
                   where info_hash = $2 and node_id = $3""",
                [ItemStatus.DONE, t.hash, self.config.node_id],
            )
        self._progress_forget(t.hash)
        self._delete_torrent_with_retry(t.hash)
        self._mediainfo_cache.pop(t.hash, None)

    def __extract_bdmv_mediainfo(self, active_objs: list[File], save_path: str) -> tuple[str, bool]:
        """Extract BDMV mediainfo from selected disc files.

        Finds a BDMV marker file among the active files and derives the disc
        path from it. Returns (media_info, hard_coded_subtitle).
        """
        disc_path = bdmv_disc_path(active_objs, save_path)
        media_info = extract_bdinfo_from_dir(self.bdinfo_bin, Path(disc_path))
        return media_info, False

    def __extract_regular_mediainfo(
        self, selected_file: TorrentFile, save_path: str
    ) -> tuple[str, bool]:
        """Extract mediainfo from the selected video file.

        Returns (media_info, hard_coded_subtitle).
        """
        path = Path(save_path, selected_file.name)
        media_info = extract_mediainfo_from_file(self.mediainfo_bin, path)
        hard_code_subtitle = check_hardcode_chinese_subtitle(
            self.ffprobe_bin, self.ffmpeg_bin, path
        )
        return media_info, hard_code_subtitle

    def _delete_torrent_with_retry(self, info_hash: str) -> None:
        for attempt in range(3):
            try:
                self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
                self._delete_torrent_files(info_hash)
                return
            except Exception as e:
                logger.warning(
                    "failed to delete torrent {} (attempt {}/3): {}, will retry",
                    info_hash,
                    attempt + 1,
                    e,
                )
                if attempt < 2:
                    time.sleep(2)
        logger.error("failed to delete torrent {} after 3 attempts, skipping cleanup", info_hash)

    def __pick_and_add_jobs(self) -> PickContext:
        logger.info("__pick_and_add_jobs")

        torrents = self.client.torrents_info()
        current_total_size = sum(t.size for t in torrents)

        left_size = int(self.config.total_process_size) - current_total_size

        if left_size <= 0:
            logger.info(
                "no space left: current={} total_limit={} left={}",
                human_readable_size(current_total_size),
                human_readable_size(self.config.total_process_size),
                human_readable_size(left_size),
            )
            return PickContext(no_space=True)

        max_count = self.config.max_downloading_count
        if max_count > 0:
            current_downloading = sum(1 for t in torrents if t.state == TorrentState.DOWNLOADING)
            pick_limit = min(max(0, max_count - current_downloading), 100)
            if pick_limit == 0:
                logger.info(
                    "at downloading count limit: downloading={} limit={}",
                    current_downloading,
                    max_count,
                )
                return PickContext(no_space=True)
        else:
            pick_limit = 100

        picked: list[tuple[int, str]] = []
        has_pending = False
        no_space = False

        with (
            self.db.connection() as conn,
            conn.transaction() as _,
        ):
            params: list[Any] = [
                self.config.single_torrent_size_limit,
                EXCLUDED_CATEGORY,
                pick_limit,
            ]

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(_pick_query(self.config), params)
                rows: list[dict[str, Any]] = cur.fetchall()

            logger.info("fetch {} rows", len(rows))
            if not rows:
                logger.info("skip pick: no pending download threads")
                return PickContext()

            if self.thread_filter_template is not None:
                before_count = len(rows)
                rows = [
                    row
                    for row in rows
                    if self.thread_filter_template.render(thread=row).strip() == "true"
                ]
                if not rows:
                    logger.info(
                        "skip pick: thread filter rejected all {} rows",
                        before_count,
                    )
                    return PickContext()

            info_hash_to_is_bdmv: dict[str, bool] = {
                row["info_hash"]: row.get("type") == "bdmv" for row in rows
            }
            info_hash_to_selected_index: dict[str, list[int]] = {
                row["info_hash"]: row["selected_index"] for row in rows
            }

            has_pending = True

            for row in rows:
                tid = row["tid"]
                info_hash = row["info_hash"]
                selected_size = row["selected_size"]
                if left_size - selected_size <= 0:
                    logger.info(
                        "skip tid={} selected_size={} left_size={}: too large",
                        tid,
                        human_readable_size(selected_size),
                        human_readable_size(left_size),
                    )
                    no_space = True
                    break

                conn.execute(
                    """
                    insert into job (tid, node_id, info_hash, start_download_time, updated_at, status, eta)
                    VALUES ($1, $2, $3, current_timestamp, current_timestamp, $4, $5)
                    """,
                    [
                        tid,
                        self.config.node_id,
                        info_hash,
                        ItemStatus.DOWNLOADING,
                        ETA_INF,
                    ],
                )
                left_size -= selected_size
                picked.append((tid, info_hash))

        if not picked:
            logger.info("pick 0 items: left_size={}", human_readable_size(left_size))
        else:
            logger.info("pick {} items", len(picked))

        # add to download client outside the lock to avoid blocking other nodes
        for tid, info_hash in picked:
            try:
                self.__add_torrent(tid, info_hash)
            except TimeoutError, ConnectionRefusedError:
                logger.warning("transient error adding torrent tid={}, will retry", tid)
                self.db.execute(
                    "delete from job where tid = $1 and node_id = $2",
                    [tid, self.config.node_id],
                )
                continue
            except Exception as e:
                logger.exception("failed to add torrent tid={}: {}", tid, e)
                self.__update_job_status(
                    status=ItemStatus.FAILED, tid=tid, failed_reason=format_exc(e)
                )
                self._progress_forget(info_hash)
                with contextlib.suppress(TorrentNotFoundError):
                    self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
                self._delete_torrent_files(info_hash)
                continue

            selected_index = info_hash_to_selected_index[info_hash]
            is_bdmv = info_hash_to_is_bdmv[info_hash]

            # Poll until torrent appears in client, then select files immediately
            for _ in range(30):
                try:
                    current = self.client.torrents_info()
                    if any(t.hash.lower() == info_hash.lower() for t in current):
                        break
                except Exception:
                    logger.debug("polling torrents_info for {} failed, retrying", info_hash)
                time.sleep(1)
            else:
                logger.error(
                    "torrent {} (tid={}) did not appear in client after 30s",
                    info_hash,
                    tid,
                )
                self.__update_job_status(
                    status=ItemStatus.FAILED,
                    tid=tid,
                    failed_reason="torrent did not appear in client after 30s",
                )
                continue

            try:
                self.__fix_file_selection_by_hash(info_hash, selected_index, is_bdmv)
            except Exception as e:
                logger.exception("failed immediate file selection for tid={}", tid)
                self.__update_job_status(
                    status=ItemStatus.FAILED, tid=tid, failed_reason=format_exc(e)
                )
                self._progress_forget(info_hash)
                with contextlib.suppress(TorrentNotFoundError):
                    self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
                self._delete_torrent_files(info_hash)
                continue

            self.client.torrents_add_tags(tags=[BT_TAG_FILE_SELECTED], torrent_hashes=info_hash)
            self.client.torrents_set_download_limit(limit=0, torrent_hashes=info_hash)
            self.client.torrents_resume(torrent_hashes=info_hash)
        return PickContext(picked=len(picked), has_pending=has_pending, no_space=no_space)

    def __maybe_evict_slowest(self) -> None:
        torrents = self.client.torrents_info()
        if not torrents:
            return

        downloading = [
            t
            for t in torrents
            if t.state not in (TorrentState.PAUSED, TorrentState.UPLOADING, TorrentState.ERRORED)
        ]
        if not downloading:
            return

        limit = int(self.config.min_download_speed)
        total_speed = sum(self._progress_avg_speed(t.hash, window=1800) or 0 for t in downloading)
        if total_speed >= limit:
            return

        now = datetime.now(tz=TZ_SHANGHAI)
        cutoff = (now - timedelta(hours=24)).timestamp()
        result = self._progress_slowest(cutoff)
        if result is None:
            return

        info_hash, avg_speed = result
        # Only evict torrents that have been downloading for at least 24h
        start_time = self.db.fetch_val(
            "select start_download_time from job where info_hash = $1 and node_id = $2",
            [info_hash, self.config.node_id],
        )
        if start_time is None or start_time.timestamp() > cutoff:
            return

        logger.info(
            "evicting slowest torrent {} (avg_speed={:.0f} B/s, limit={} B/s)",
            info_hash,
            avg_speed,
            limit,
        )
        self.__update_job_status(
            status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
            info_hash=info_hash,
            removed_reason="slow_download",
        )
        self.db.execute(
            "update thread set torrent_invalid = 'stalled' where info_hash = $1",
            [info_hash],
        )
        self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)

    def __add_torrent(
        self,
        tid: int,
        info_hash: str,
    ) -> None:
        tc = self.store.read(tid)
        if not tc:
            self.__update_job_status(
                status=ItemStatus.FAILED,
                tid=tid,
                failed_reason="torrent content not found",
            )
            return

        tc = set_torrent_comment(tc, f"https://{MTeamDomain}/detail/{tid}")
        logger.info("added torrent tid={} info_hash={}", tid, info_hash)

        r = self.client.torrents_add(
            torrent_files=[tc],
            save_path=os.path.join(self.config.download_path, info_hash),
            use_auto_torrent_management=False,
            tags=[BT_TAG_DOWNLOADING],
            download_limit=1,
            is_sequential_download=True,
        )
        if r != "Ok.":
            self.__update_job_status(
                status=ItemStatus.FAILED, tid=tid, failed_reason="failed to add"
            )
            with contextlib.suppress(TorrentNotFoundError):
                self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
            self._delete_torrent_files(info_hash)
            return
        # Record initial progress sample for stalled detection
        self._progress_record(info_hash, 0)
