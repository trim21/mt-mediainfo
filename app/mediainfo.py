import subprocess
import tempfile
from pathlib import Path

from sslog import logger

from app.utils import must_find_executable, must_run_command

mediainfo = must_find_executable("mediainfo")
logger.info("using mediainfo at {!r}", mediainfo)


def extract_mediainfo_from_file(file: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="mt-") as tempdir:
        out_file = Path(tempdir, "mediainfo.txt")
        must_run_command(
            mediainfo,
            [f"--LogFile={out_file}", file.name],
            cwd=str(file.parent),
            stdout=subprocess.DEVNULL,
        )
        return out_file.read_text("utf-8")
