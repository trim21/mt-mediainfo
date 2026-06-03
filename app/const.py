import enum
from datetime import datetime
from typing import Final, LiteralString, cast
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
BT_TAG_NEED_SELECT: Final = "need-select"


class PickStrategy(str, enum.Enum):
    tid = "tid"  # priority category first, then tid asc
    seeders = "seeders"  # seeders desc, then priority category, then tid asc


def pick_order_clause(strategy: PickStrategy, priority_category_param: int) -> LiteralString:
    if strategy == PickStrategy.seeders:
        return cast(
            LiteralString,
            f"order by seeders desc, (category = any(${priority_category_param})) desc, selected_size asc, tid asc",
        )
    return cast(
        LiteralString,
        f"order by tid asc, (category = any(${priority_category_param})) desc",
    )


SELECTED_CATEGORY = [
    401,
    419,
    420,
    421,
    439,
    403,
    402,
    438,
    435,
    404,
    406,
    405,
    407,
]

PRIORITY_CATEGORY = [
    *SELECTED_CATEGORY,
    # 401,
    # 419,
    # 420,
    # 421,
    # 439,
]


def search_cursor_key(mode: str) -> str:
    now = datetime.now(TZ_SHANGHAI)
    return f"search_cursor:{now.year}.{now.month % 4}:{mode}"
