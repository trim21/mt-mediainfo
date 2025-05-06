from typing import Final

LOCK_KEY_SCHEDULE_RSS: Final = "schedule"
LOCK_KEY_PICK_RSS_JOB: Final = "pick-rss-job"

# status of rss_run
TASK_STATUS_PENDING: Final = "pending"
TASK_STATUS_RUNNING: Final = "running"
TASK_STATUS_SUCCESS: Final = "success"
TASK_STATUS_FAILED: Final = "failed"

# status of rss_item
ITEM_STATUS_PENDING: Final = "pending"
ITEM_STATUS_SKIPPED: Final = "skipped"
ITEM_STATUS_DOWNLOADING: Final = "downloading"  # 下载中
ITEM_STATUS_UPLOADING: Final = "uploading"  # 已发帖
ITEM_STATUS_DONE: Final = "done"  # 已出种
ITEM_STATUS_REMOVED_FROM_SITE: Final = "removed-by-site"  # 被站点删除
ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT: Final = "removed-by-client"  # 被从客户端删除
ITEM_STATUS_FAILED: Final = "failed"

ITEM_STATUS_PROCESSING: Final = (
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_UPLOADING,
)

VIDEO_FILE_EXT = (".mkv", ".mp4", ".avi", ".wmv")

MOVIE_CATEGORY = {401, 419, 420, 421, 439}

SELECTED_CATEGORY = list(MOVIE_CATEGORY)

# SELECTED_CATEGORY = {
#     401,
#     402,
#     403,
#     404,
#     405,
#     407,
#     410,
#     412,
#     419,
#     420,
#     421,
#     424,
#     425,
#     426,
#     429,
#     430,
#     431,
#     432,
#     435,
#     436,
#     437,
#     438,
#     439,
#     440,
# }
