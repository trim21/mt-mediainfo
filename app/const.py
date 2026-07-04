import enum
from datetime import datetime
from typing import Final, LiteralString
from zoneinfo import ZoneInfo

TZ_SHANGHAI: Final = ZoneInfo("Asia/Shanghai")

LOCK_KEY_SCHEDULE_RSS: Final = "schedule"
LOCK_KEY_PICK_RSS_JOB: Final = "pick-job"


class ItemStatus(enum.StrEnum):
    PENDING = "pending"
    SKIPPED = "skipped"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    DONE = "done"
    REMOVED_FROM_SITE = "removed-by-site"
    REMOVED_FROM_DOWNLOAD_CLIENT = "removed-by-client"
    FAILED = "failed"


ITEM_STATUS_PROCESSING: Final = (
    ItemStatus.DOWNLOADING,
    ItemStatus.UPLOADING,
)

VIDEO_FILE_EXT = (".mkv", ".mp4", ".avi", ".wmv")

BT_TAG_PROCESS_ERROR: Final = "process-error"
BT_TAG_SELECTING_FILES: Final = "selecting-files"
BT_TAG_DOWNLOADING: Final = "downloading"
BT_TAG_PROCESSING: Final = "processing"
BT_TAG_FILE_SELECTED: Final = "file-selected-4"
BT_TAG_QUEUED: Final = "queued"


class PickStrategy(str, enum.Enum):
    tid = "tid"
    seeders = "seeders"


def pick_order_clause(strategy: PickStrategy) -> LiteralString:
    if strategy == PickStrategy.seeders:
        return "order by seeders desc, priority desc, selected_size asc, tid asc"
    return "order by tid asc, priority desc"


def search_cursor_key(mode: str) -> str:
    now = datetime.now(TZ_SHANGHAI)
    return f"search_cursor:{now.year}.{now.month % 4}:{mode}"


EXCLUDED_CATEGORY = [427]
