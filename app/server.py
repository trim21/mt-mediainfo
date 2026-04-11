import asyncio
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Protocol, cast
from zoneinfo import ZoneInfo

import asyncpg
import fastapi
import orjson
from fastapi import Depends, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, JSONResponse

from app.config import Config, load_config
from app.const import (
    ITEM_STATUS_DONE,
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_FAILED,
    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
    SELECTED_CATEGORY,
)
from app.utils import human_readable_byte_rate, human_readable_size


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_INDENT_2, default=str)


_tz_shanghai = ZoneInfo("Asia/Shanghai")

templates = Jinja2Templates(directory=str(Path(__file__).parent.joinpath("templates").resolve()))


class _Render(Protocol):
    def __call__(
        self,
        name: str,
        ctx: dict[str, Any] | None = ...,
        status_code: int = ...,
        headers: Mapping[str, str] | None = ...,
        media_type: str | None = ...,
    ) -> HTMLResponse: ...


async def __render(request: Request) -> _Render:
    def render(
        name: str,
        ctx: dict[str, Any] | None = None,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            name=name,
            request=request,
            context=ctx,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
        )

    return render


type Render = Annotated[_Render, Depends(__render)]


def create_app() -> fastapi.FastAPI:
    cfg: Config = load_config()

    pool = asyncpg.create_pool(cfg.pg_dsn())

    @asynccontextmanager
    async def lifespan(_app: fastapi.FastAPI) -> AsyncGenerator[None, None]:
        await pool
        yield
        await pool.close()

    app = fastapi.FastAPI(debug=cfg.debug, lifespan=lifespan)

    def _fmt_dt(dt: datetime | None) -> str:
        if dt is None:
            return "-"
        return dt.astimezone(_tz_shanghai).strftime("%Y-%m-%d %H:%M:%S")

    templates.env.filters["fmt_dt"] = _fmt_dt

    def _today_start() -> datetime:
        now = datetime.now(_tz_shanghai)
        return datetime(now.year, now.month, now.day, tzinfo=_tz_shanghai)

    def _week_range() -> tuple[int, int]:
        """Return (min_week_num, max_week_num) for last 52 weeks."""
        return -52, -1

    def _week_label(today: datetime, week_num: int) -> str:
        ref = today + timedelta(days=1)
        start = ref + timedelta(weeks=int(week_num))
        return start.strftime("%Y-%m-%d")

    _backfill_lock = asyncio.Lock()

    async def _backfill_daily_stats(since: date) -> None:
        async with _backfill_lock:
            today_date = _today_start().date()
            yesterday = today_date - timedelta(days=1)
            start_date = since if since < today_date else yesterday

            if start_date > yesterday:
                return

            # Find which days in the range are already cached
            existing = await pool.fetch(
                "select day from daily_stats where day >= $1 and day <= $2",
                start_date,
                yesterday,
            )
            existing_days = {r["day"] for r in existing}

            missing_days = [
                start_date + timedelta(days=i)
                for i in range((yesterday - start_date).days + 1)
                if (start_date + timedelta(days=i)) not in existing_days
            ]
            if not missing_days:
                return

            first_missing = missing_days[0]
            last_missing = missing_days[-1]
            start_ts = datetime(
                first_missing.year, first_missing.month, first_missing.day, tzinfo=_tz_shanghai
            )
            end_ts = datetime(
                last_missing.year, last_missing.month, last_missing.day, tzinfo=_tz_shanghai
            ) + timedelta(days=1)

            (
                downloaded_rows,
                fetched_rows,
                thread_rows,
                torrent_rows,
                mediainfo_rows,
            ) = await asyncio.gather(
                pool.fetch(
                    """
                    select (coalesce(job.completed_at, job.updated_at)
                            at time zone 'Asia/Shanghai')::date as day,
                           job.node_id::text as node_id,
                           count(1)::int as count,
                           coalesce(sum(thread.selected_size), 0)::int8 as bytes
                    from job
                    join thread on (thread.tid = job.tid)
                    where job.status = $1 and thread.selected_size > 0
                      and coalesce(job.completed_at, job.updated_at) >= $2
                      and coalesce(job.completed_at, job.updated_at) < $3
                    group by day, job.node_id
                    """,
                    ITEM_STATUS_DONE,
                    start_ts,
                    end_ts,
                ),
                pool.fetch(
                    """
                    select (torrent_fetched_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count,
                           coalesce(sum(selected_size), 0)::int8 as bytes
                    from thread
                    where torrent_fetched_at >= $1 and torrent_fetched_at < $2
                      and selected_size > 0 and category = any($3)
                    group by day
                    """,
                    start_ts,
                    end_ts,
                    SELECTED_CATEGORY,
                ),
                pool.fetch(
                    """
                    select (created_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count
                    from thread
                    where created_at >= $1 and created_at < $2
                    group by day
                    """,
                    start_ts,
                    end_ts,
                ),
                pool.fetch(
                    """
                    select (created_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count
                    from torrent
                    where created_at >= $1 and created_at < $2
                    group by day
                    """,
                    start_ts,
                    end_ts,
                ),
                pool.fetch(
                    """
                    select (mediainfo_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count
                    from thread
                    where mediainfo_at >= $1 and mediainfo_at < $2
                    group by day
                    """,
                    start_ts,
                    end_ts,
                ),
            )

            days_data: dict[date, dict[str, Any]] = {}

            def _ensure_day(d: date) -> dict[str, Any]:
                if d not in days_data:
                    days_data[d] = {
                        "downloaded_bytes": 0,
                        "downloaded_count": 0,
                        "fetched_bytes": 0,
                        "fetched_count": 0,
                        "thread_count": 0,
                        "torrent_count": 0,
                        "mediainfo_count": 0,
                        "node_downloaded": {},
                    }
                return days_data[d]

            for r in downloaded_rows:
                d = _ensure_day(r["day"])
                d["downloaded_bytes"] += r["bytes"]
                d["downloaded_count"] += r["count"]
                d["node_downloaded"][r["node_id"]] = {
                    "bytes": int(r["bytes"]),
                    "count": int(r["count"]),
                }

            for r in fetched_rows:
                d = _ensure_day(r["day"])
                d["fetched_bytes"] = int(r["bytes"])
                d["fetched_count"] = int(r["count"])

            for r in thread_rows:
                _ensure_day(r["day"])["thread_count"] = int(r["count"])

            for r in torrent_rows:
                _ensure_day(r["day"])["torrent_count"] = int(r["count"])

            for r in mediainfo_rows:
                _ensure_day(r["day"])["mediainfo_count"] = int(r["count"])

            rows_to_insert: list[tuple[Any, ...]] = []
            for current in missing_days:
                data = days_data.get(current)
                if data:
                    rows_to_insert.append((
                        current,
                        data["downloaded_bytes"],
                        data["downloaded_count"],
                        data["fetched_bytes"],
                        data["fetched_count"],
                        data["thread_count"],
                        data["torrent_count"],
                        data["mediainfo_count"],
                        orjson.dumps(data["node_downloaded"]).decode(),
                    ))
                else:
                    rows_to_insert.append((current, 0, 0, 0, 0, 0, 0, 0, "{}"))

            if rows_to_insert:
                await pool.executemany(
                    """
                    insert into daily_stats
                        (day, downloaded_bytes, downloaded_count,
                         fetched_bytes, fetched_count, thread_count,
                         torrent_count, mediainfo_count, node_downloaded)
                    values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                    on conflict (day) do update set
                        downloaded_bytes = excluded.downloaded_bytes,
                        downloaded_count = excluded.downloaded_count,
                        fetched_bytes = excluded.fetched_bytes,
                        fetched_count = excluded.fetched_count,
                        thread_count = excluded.thread_count,
                        torrent_count = excluded.torrent_count,
                        mediainfo_count = excluded.mediainfo_count,
                        node_downloaded = excluded.node_downloaded
                    """,
                    rows_to_insert,
                )

    async def _compute_today_stats() -> dict[str, Any]:
        today = _today_start()
        tomorrow = today + timedelta(days=1)

        (
            downloaded_rows,
            fetched_row,
            thread_count,
            torrent_count,
            mediainfo_count,
        ) = await asyncio.gather(
            pool.fetch(
                """
                select job.node_id::text as node_id,
                       count(1)::int as count,
                       coalesce(sum(thread.selected_size), 0)::int8 as bytes
                from job
                join thread on (thread.tid = job.tid)
                where job.status = $1 and thread.selected_size > 0
                  and coalesce(job.completed_at, job.updated_at) >= $2
                  and coalesce(job.completed_at, job.updated_at) < $3
                group by job.node_id
                """,
                ITEM_STATUS_DONE,
                today,
                tomorrow,
            ),
            pool.fetchrow(
                """
                select count(1)::int as count,
                       coalesce(sum(selected_size), 0)::int8 as bytes
                from thread
                where torrent_fetched_at >= $1 and torrent_fetched_at < $2
                  and selected_size > 0 and category = any($3)
                """,
                today,
                tomorrow,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                "select count(1)::int from thread where created_at >= $1 and created_at < $2",
                today,
                tomorrow,
            ),
            pool.fetchval(
                "select count(1)::int from torrent where created_at >= $1 and created_at < $2",
                today,
                tomorrow,
            ),
            pool.fetchval(
                "select count(1)::int from thread where mediainfo_at >= $1 and mediainfo_at < $2",
                today,
                tomorrow,
            ),
        )

        node_downloaded: dict[str, dict[str, int]] = {}
        total_downloaded_bytes = 0
        total_downloaded_count = 0
        for r in downloaded_rows:
            b = int(r["bytes"])
            c = int(r["count"])
            total_downloaded_bytes += b
            total_downloaded_count += c
            node_downloaded[r["node_id"]] = {"bytes": b, "count": c}

        fetched_row = cast(asyncpg.Record, fetched_row)

        return {
            "day": today.date(),
            "downloaded_bytes": total_downloaded_bytes,
            "downloaded_count": total_downloaded_count,
            "fetched_bytes": int(fetched_row["bytes"]),
            "fetched_count": int(fetched_row["count"]),
            "thread_count": int(thread_count),
            "torrent_count": int(torrent_count),
            "mediainfo_count": int(mediainfo_count),
            "node_downloaded": node_downloaded,
        }

    def _stats_from_record(r: asyncpg.Record) -> dict[str, Any]:
        return {
            "downloaded_bytes": r["downloaded_bytes"],
            "downloaded_count": r["downloaded_count"],
            "fetched_bytes": r["fetched_bytes"],
            "fetched_count": r["fetched_count"],
            "thread_count": r["thread_count"],
            "torrent_count": r["torrent_count"],
            "mediainfo_count": r["mediainfo_count"],
            "node_downloaded": r["node_downloaded"] or {},
        }

    def _build_weekly_charts(
        history_rows: list[asyncpg.Record],
        today_stats: dict[str, Any],
    ) -> dict[str, Any]:
        today = _today_start()
        today_date = today_stats["day"]
        ref_date = today_date + timedelta(days=1)
        min_week, max_week = _week_range()

        by_day: dict[date, dict[str, Any]] = {r["day"]: _stats_from_record(r) for r in history_rows}
        by_day[today_date] = today_stats

        week_days: dict[int, list[dict[str, Any]]] = {w: [] for w in range(min_week, max_week + 1)}
        for d, s in by_day.items():
            wn = (d - ref_date).days // 7
            if min_week <= wn <= max_week:
                week_days[wn].append(s)

        all_node_ids: set[str] = set()
        for s in by_day.values():
            all_node_ids.update(s.get("node_downloaded", {}).keys())
        sorted_node_ids = sorted(all_node_ids)

        labels: list[str] = []
        byte_rate_totals: list[dict[str, Any]] = []
        done_count_totals: list[int] = []
        fetched_data: list[dict[str, Any]] = []
        thread_count_data: list[dict[str, Any]] = []
        torrent_count_data: list[dict[str, Any]] = []
        mediainfo_count_data: list[dict[str, Any]] = []

        byte_rate_per_node: dict[str, list[float]] = {nid: [] for nid in sorted_node_ids}
        done_count_per_node: dict[str, list[int]] = {nid: [] for nid in sorted_node_ids}

        for wn in range(min_week, max_week + 1):
            label = _week_label(today, wn)
            labels.append(label)
            is_current_week = wn == max_week
            days = week_days[wn]

            total_dl_bytes = sum(s["downloaded_bytes"] for s in days)
            total_dl_count = sum(s["downloaded_count"] for s in days)
            total_fetched_bytes = sum(s["fetched_bytes"] for s in days)
            total_fetched_count = sum(s["fetched_count"] for s in days)

            byte_rate = total_dl_bytes / (7.0 * 86400)
            byte_rate_totals.append({
                "byte_rate": byte_rate,
                "byte_rate_fmt": human_readable_byte_rate(byte_rate),
                "total_size": int(total_dl_bytes),
                "total_size_fmt": human_readable_size(total_dl_bytes),
            })
            done_count_totals.append(total_dl_count)

            if is_current_week and total_fetched_count > 0:
                fetched_rate = (
                    (total_fetched_bytes / total_fetched_count) * 1200 * 7 / (7.0 * 86400)
                )
            else:
                fetched_rate = total_fetched_bytes / (7.0 * 86400)
            fetched_data.append({"week": label, "byte_rate": fetched_rate})

            thread_count_data.append({
                "week": label,
                "count": sum(s["thread_count"] for s in days),
            })
            torrent_count_data.append({
                "week": label,
                "count": sum(s["torrent_count"] for s in days),
            })
            mediainfo_count_data.append({
                "week": label,
                "count": sum(s["mediainfo_count"] for s in days),
            })

            node_totals: dict[str, dict[str, int]] = {}
            for s in days:
                for nid, nd in s.get("node_downloaded", {}).items():
                    if nid not in node_totals:
                        node_totals[nid] = {"bytes": 0, "count": 0}
                    node_totals[nid]["bytes"] += nd.get("bytes", 0)
                    node_totals[nid]["count"] += nd.get("count", 0)

            for nid in sorted_node_ids:
                nt = node_totals.get(nid, {"bytes": 0, "count": 0})
                byte_rate_per_node[nid].append(nt["bytes"] / (7.0 * 86400))
                done_count_per_node[nid].append(nt["count"])

        return {
            "weekly_byte_rate": {
                "labels": labels,
                "totals": byte_rate_totals,
                "per_node": byte_rate_per_node,
            },
            "weekly_fetched_size": fetched_data,
            "weekly_thread_count": thread_count_data,
            "weekly_torrent_count": torrent_count_data,
            "weekly_done_count": {
                "labels": labels,
                "totals": done_count_totals,
                "per_node": done_count_per_node,
            },
            "weekly_mediainfo_count": mediainfo_count_data,
        }

    def _build_daily_charts(
        history_rows: list[asyncpg.Record],
        today_stats: dict[str, Any],
        days_back: int,
    ) -> dict[str, Any]:
        today_date = today_stats["day"]
        elapsed_today = max((datetime.now(_tz_shanghai) - _today_start()).total_seconds(), 1.0)

        by_day: dict[date, dict[str, Any]] = {r["day"]: _stats_from_record(r) for r in history_rows}
        by_day[today_date] = today_stats

        all_node_ids: set[str] = set()
        for v in by_day.values():
            all_node_ids.update(v.get("node_downloaded", {}).keys())
        sorted_node_ids = sorted(all_node_ids)

        labels: list[str] = []
        byte_rate_totals: list[dict[str, Any]] = []
        done_count_totals: list[int] = []
        fetched_data: list[dict[str, Any]] = []
        thread_count_data: list[dict[str, Any]] = []
        torrent_count_data: list[dict[str, Any]] = []
        mediainfo_count_data: list[dict[str, Any]] = []

        byte_rate_per_node: dict[str, list[float]] = {nid: [] for nid in sorted_node_ids}
        done_count_per_node: dict[str, list[int]] = {nid: [] for nid in sorted_node_ids}

        for i in range(-days_back, 1):
            d = today_date + timedelta(days=i)
            label = d.strftime("%Y-%m-%d")
            labels.append(label)
            is_today = d == today_date
            s = by_day.get(d)

            if s is None:
                byte_rate_totals.append({
                    "byte_rate": 0.0,
                    "byte_rate_fmt": human_readable_byte_rate(0),
                    "total_size": 0,
                    "total_size_fmt": human_readable_size(0),
                })
                done_count_totals.append(0)
                fetched_data.append({"day": label, "byte_rate": 0.0})
                thread_count_data.append({"day": label, "count": 0})
                torrent_count_data.append({"day": label, "count": 0})
                mediainfo_count_data.append({"day": label, "count": 0})
                for nid in sorted_node_ids:
                    byte_rate_per_node[nid].append(0.0)
                    done_count_per_node[nid].append(0)
                continue

            dl_bytes = s["downloaded_bytes"]
            byte_rate = dl_bytes / elapsed_today if is_today else dl_bytes / 86400.0
            byte_rate_totals.append({
                "byte_rate": byte_rate,
                "byte_rate_fmt": human_readable_byte_rate(byte_rate),
                "total_size": int(dl_bytes),
                "total_size_fmt": human_readable_size(dl_bytes),
            })
            done_count_totals.append(s["downloaded_count"])

            f_bytes = s["fetched_bytes"]
            f_count = s["fetched_count"]
            if is_today and f_count > 0:
                fetched_rate = (f_bytes / f_count) * 1200 / 86400.0
            else:
                fetched_rate = f_bytes / 86400.0
            fetched_data.append({"day": label, "byte_rate": fetched_rate})

            thread_count_data.append({"day": label, "count": s["thread_count"]})
            torrent_count_data.append({"day": label, "count": s["torrent_count"]})
            mediainfo_count_data.append({"day": label, "count": s["mediainfo_count"]})

            nd = s.get("node_downloaded", {})
            for nid in sorted_node_ids:
                n = nd.get(nid, {})
                n_bytes = n.get("bytes", 0)
                byte_rate_per_node[nid].append(
                    n_bytes / elapsed_today if is_today else n_bytes / 86400.0
                )
                done_count_per_node[nid].append(n.get("count", 0))

        return {
            "daily_byte_rate": {
                "labels": labels,
                "totals": byte_rate_totals,
                "per_node": byte_rate_per_node,
            },
            "daily_fetched_size": fetched_data,
            "daily_thread_count": thread_count_data,
            "daily_torrent_count": torrent_count_data,
            "daily_done_count": {
                "labels": labels,
                "totals": done_count_totals,
                "per_node": done_count_per_node,
            },
            "daily_mediainfo_count": mediainfo_count_data,
        }

    @app.get("/")
    async def progress(render: Render) -> HTMLResponse:
        (
            thread_stats,
            search_cursor,
            pending_download_stats,
            all_job_rows,
        ) = await asyncio.gather(
            pool.fetchrow(
                """
            select
              count(1) as scraped_total,
              count(1) filter (where category = any($1)) as total,
              coalesce(sum(selected_size) filter (where category = any($1)), 0) as total_size,
              count(1) filter (where deleted = false and mediainfo_at is null
                and upload_at >= '2024-01-01' and category = any($1)) as pending_fetch_mediainfo,
              count(1) filter (where deleted = false and mediainfo_at is not null
                and mediainfo = '' and info_hash = '' and category = any($1)) as pending_fetch_torrent,
              count(1) filter (where mediainfo != '' and info_hash != ''
                and category = any($1)) as done,
              coalesce(sum(selected_size) filter (where mediainfo != '' and info_hash != ''
                and category = any($1)), 0) as done_size
            from thread
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetchval("select value from config where key = 'search_cursor'"),
            pool.fetchrow(
                """
            select count(1) as count, coalesce(sum(thread.selected_size), 0) as size
            from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetch(
                """
            select job.status, job.node_id, thread.selected_size from job
            join thread on (thread.tid = job.tid)
            where thread.category = any($1)
              and job.status = any($2)
            """,
                SELECTED_CATEGORY,
                [
                    ITEM_STATUS_DOWNLOADING,
                    ITEM_STATUS_DONE,
                    ITEM_STATUS_FAILED,
                    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                ],
            ),
        )

        thread_stats = cast(asyncpg.Record, thread_stats)
        scraped_total = cast(int, thread_stats["scraped_total"])
        total = cast(int, thread_stats["total"])
        total_size = cast(int, thread_stats["total_size"])
        pending_fetch_mediainfo = cast(int, thread_stats["pending_fetch_mediainfo"])
        pending_fetch_torrent = cast(int, thread_stats["pending_fetch_torrent"])
        done = cast(int, thread_stats["done"])
        done_size = cast(int, thread_stats["done_size"])

        pending_download_stats = cast(asyncpg.Record, pending_download_stats)
        pending_to_download = cast(int, pending_download_stats["count"])
        pending_to_download_size = cast(int, pending_download_stats["size"])

        downloading_rows = [r for r in all_job_rows if r["status"] == ITEM_STATUS_DOWNLOADING]
        done_job_rows = [r for r in all_job_rows if r["status"] == ITEM_STATUS_DONE]
        failed = sum(1 for r in all_job_rows if r["status"] == ITEM_STATUS_FAILED)
        failed_size = sum(
            r["selected_size"] for r in all_job_rows if r["status"] == ITEM_STATUS_FAILED
        )
        removed_by_client = sum(
            1 for r in all_job_rows if r["status"] == ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT
        )
        removed_by_client_size = sum(
            r["selected_size"]
            for r in all_job_rows
            if r["status"] == ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT
        )

        downloading = len(downloading_rows)
        downloading_size = sum(r["selected_size"] for r in downloading_rows)

        # Per-node downloading stats
        downloading_per_node: dict[str, dict[str, Any]] = {}
        for r in downloading_rows:
            nid = str(r["node_id"])
            if nid not in downloading_per_node:
                downloading_per_node[nid] = {"count": 0, "size": 0}
            downloading_per_node[nid]["count"] += 1
            downloading_per_node[nid]["size"] += r["selected_size"]

        downloading_nodes = [
            {"node_id": nid[:8], "count": v["count"], "size_fmt": human_readable_size(v["size"])}
            for nid, v in sorted(downloading_per_node.items())
        ]

        done_per_node: dict[str, dict[str, Any]] = {}
        for r in done_job_rows:
            nid = str(r["node_id"])
            if nid not in done_per_node:
                done_per_node[nid] = {"count": 0, "size": 0}
            done_per_node[nid]["count"] += 1
            done_per_node[nid]["size"] += r["selected_size"]

        done_nodes = [
            {"node_id": nid[:8], "count": v["count"], "size_fmt": human_readable_size(v["size"])}
            for nid, v in sorted(done_per_node.items())
        ]

        skipped = (
            total
            - done
            - downloading
            - failed
            - removed_by_client
            - pending_fetch_mediainfo
            - pending_fetch_torrent
            - pending_to_download
        )

        skipped_size = (
            total_size - done_size - downloading_size - failed_size - removed_by_client_size
        )

        def pct(n: int) -> str:
            if total == 0:
                return "0.0"
            return f"{n / total * 100:.1f}"

        return render(
            "index.html.j2",
            ctx={
                "scraped_total": scraped_total,
                "search_cursor": search_cursor or "N/A",
                "total": total,
                "total_size": human_readable_size(total_size),
                "done": done,
                "done_size": human_readable_size(done_size),
                "done_pct": pct(done),
                "done_nodes": done_nodes,
                "pending_fetch_mediainfo": pending_fetch_mediainfo,
                "pending_fetch_mediainfo_pct": pct(pending_fetch_mediainfo),
                "pending_fetch_torrent": pending_fetch_torrent,
                "pending_fetch_torrent_pct": pct(pending_fetch_torrent),
                "pending_to_download": pending_to_download,
                "pending_to_download_size": human_readable_size(pending_to_download_size),
                "pending_to_download_pct": pct(pending_to_download),
                "downloading": downloading,
                "downloading_size": human_readable_size(downloading_size),
                "downloading_pct": pct(downloading),
                "downloading_nodes": downloading_nodes,
                "failed": failed,
                "failed_size": human_readable_size(failed_size),
                "failed_pct": pct(failed),
                "removed_by_client": removed_by_client,
                "removed_by_client_size": human_readable_size(removed_by_client_size),
                "removed_by_client_pct": pct(removed_by_client),
                "skipped": skipped,
                "skipped_size": human_readable_size(skipped_size),
                "skipped_pct": pct(skipped),
            },
        )

    @app.get("/api/weekly-charts")
    async def weekly_charts() -> ORJSONResponse:
        since = _today_start().date() - timedelta(days=365)
        await _backfill_daily_stats(since)
        history_rows, today_stats = await asyncio.gather(
            pool.fetch(
                "select * from daily_stats where day >= $1 order by day",
                since,
            ),
            _compute_today_stats(),
        )
        return ORJSONResponse(_build_weekly_charts(history_rows, today_stats))

    @app.get("/detail")
    async def detail(render: Render, start: str | None = None) -> HTMLResponse:
        today = _today_start()
        default_start = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        start_value = start if start is not None else default_start

        start_dt: datetime | None = None
        if start_value:
            start_dt = datetime.strptime(start_value, "%Y-%m-%d").replace(tzinfo=_tz_shanghai)

        if start_dt is None:
            start_dt = today - timedelta(days=364)
        start_dt = start_dt.replace(tzinfo=_tz_shanghai) if start_dt.tzinfo is None else start_dt
        days_back = (today - start_dt).days

        await _backfill_daily_stats(start_dt.date())
        history_rows, today_stats = await asyncio.gather(
            pool.fetch(
                "select * from daily_stats where day >= $1 order by day",
                start_dt.date(),
            ),
            _compute_today_stats(),
        )
        charts = _build_daily_charts(history_rows, today_stats, days_back)

        return render(
            "detail.html.j2",
            ctx={"start": start_value} | charts,
        )

    @app.get("/threads/pending-mediainfo")
    async def threads_pending_mediainfo(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select tid, category, size, selected_size, seeders, created_at from thread
            where deleted = false and mediainfo_at is null
              and upload_at >= '2024-01-01' and category = any($1)
            order by tid desc
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Pending Fetch Mediainfo",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/pending-torrent")
    async def threads_pending_torrent(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select tid, category, size, selected_size, seeders, created_at from thread
            where deleted = false and mediainfo_at is not null
              and mediainfo = '' and info_hash = '' and category = any($1)
            order by tid desc
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Pending Fetch Torrent",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/pending-download")
    async def threads_pending_download(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            order by selected_size desc
            limit 100
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Pending to Download",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"]),
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/downloading")
    async def threads_downloading(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at,
                   job.progress, job.node_id
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            """,
            ITEM_STATUS_DOWNLOADING,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Downloading",
                "show_progress": True,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/done")
    async def threads_done(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select tid, category, size, selected_size, seeders, created_at from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            order by tid desc
            limit 100
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Done",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/failed")
    async def threads_failed(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at,
                   job.failed_reason
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            """,
            ITEM_STATUS_FAILED,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Failed",
                "show_progress": False,
                "show_failed_reason": True,
                "show_reset": True,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/removed")
    async def threads_removed(render: Render) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            """,
            ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Removed by Client",
                "show_progress": False,
                "show_failed_reason": False,
                "show_reset": True,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.post("/api/thread/{tid}/reset")
    async def reset_thread(tid: int) -> ORJSONResponse:
        result = await pool.execute(
            """
            delete from job
            where tid = $1 and status = any($2)
            """,
            tid,
            [ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT, ITEM_STATUS_FAILED],
        )
        return ORJSONResponse({"deleted": result})

    @app.get("/nodes")
    async def nodes_page(render: Render) -> HTMLResponse:
        node_rows = await pool.fetch("select id, last_seen from node order by last_seen desc")
        job_rows = await pool.fetch(
            "select node_id, status, count(1) as cnt from job group by node_id, status"
        )

        counts: dict[str, dict[str, int]] = {}
        for r in job_rows:
            nid = str(r["node_id"])
            counts.setdefault(nid, {})
            counts[nid][r["status"]] = r["cnt"]

        nodes_data = [
            {
                "id": str(n["id"]),
                "last_seen": n["last_seen"],
                "downloading": counts.get(str(n["id"]), {}).get(ITEM_STATUS_DOWNLOADING, 0),
                "done": counts.get(str(n["id"]), {}).get(ITEM_STATUS_DONE, 0),
                "failed": counts.get(str(n["id"]), {}).get(ITEM_STATUS_FAILED, 0),
                "removed": counts.get(str(n["id"]), {}).get(
                    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT, 0
                ),
                "total": sum(counts.get(str(n["id"]), {}).values()),
            }
            for n in node_rows
        ]

        return render("nodes.html.j2", ctx={"nodes": nodes_data})

    def _fmt_eta(seconds: float) -> str:
        if seconds <= 0:
            return "∞"
        if seconds < 60:
            return f"{int(seconds)}s"
        if seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"

    @app.get("/nodes/{node_id}")
    async def node_jobs_page(node_id: str, render: Render) -> HTMLResponse:
        node_row = await pool.fetchrow("select id, last_seen from node where id = $1", node_id)
        if node_row is None:
            return render("nodes.html.j2", ctx={"nodes": []}, status_code=404)

        rows = await pool.fetch(
            """
            select job.tid, job.status, job.progress, job.failed_reason,
                   job.start_download_time, job.updated_at,
                   thread.size, thread.selected_size
            from job
            join thread on (thread.tid = job.tid)
            where job.node_id = $1 and job.status = $2
            """,
            node_id,
            ITEM_STATUS_DOWNLOADING,
        )

        now = datetime.now(UTC)

        def _calc_speed_eta(r: asyncpg.Record) -> dict[str, Any]:
            selected_size: int = r["selected_size"]
            progress: float = r["progress"]
            start: datetime | None = r["start_download_time"]
            if not start or selected_size <= 0 or progress <= 0:
                return {"speed_fmt": "-", "eta_fmt": "-", "eta_seconds": float("inf")}
            elapsed = (now - start).total_seconds()
            if elapsed <= 0:
                return {"speed_fmt": "-", "eta_fmt": "-", "eta_seconds": float("inf")}
            bytes_done = selected_size * progress
            speed = bytes_done / elapsed
            remaining = selected_size * (1 - progress)
            eta_seconds = remaining / speed if speed > 0 else float("inf")
            return {
                "speed_fmt": human_readable_byte_rate(speed),
                "eta_fmt": _fmt_eta(eta_seconds),
                "eta_seconds": eta_seconds,
            }

        jobs = sorted(
            [
                dict(r)
                | {
                    "size_fmt": human_readable_size(r["size"]),
                    "selected_size_fmt": human_readable_size(r["selected_size"])
                    if r["selected_size"] > 0
                    else "-",
                    "progress_fmt": f"{int(r['progress'] * 1000) / 10:.1f}",
                }
                | _calc_speed_eta(r)
                for r in rows
            ],
            key=lambda j: j["eta_seconds"],
        )

        return render(
            "node_jobs.html.j2",
            ctx={
                "node_id": str(node_row["id"]),
                "last_seen": node_row["last_seen"],
                "jobs": jobs,
            },
        )

    return app
