import enum
from typing import Final

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

QB_TAG_PROCESS_ERROR: Final = "process-error"
QB_TAG_SELECTING_FILES: Final = "selecting-files"
QB_TAG_DOWNLOADING: Final = "downloading"
QB_TAG_PROCESSING: Final = "processing"
QB_TAG_NEED_SELECT: Final = "need-select"


class PickStrategy(str, enum.Enum):
    tid = "tid"  # priority category first, then tid asc
    seeders = "seeders"  # seeders desc, then priority category, then tid asc


class SeederFilter(str, enum.Enum):
    gte = "gte"
    lt = "lt"


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
