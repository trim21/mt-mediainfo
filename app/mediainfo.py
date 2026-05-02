import subprocess
import tempfile
from pathlib import Path

from app.utils import must_run_command


def extract_mediainfo_from_file(mediainfo_bin: str, file: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="mt-") as tempdir:
        out_file = Path(tempdir, "mediainfo.txt")
        must_run_command(
            mediainfo_bin,
            [f"--LogFile={out_file}", file.name],
            cwd=str(file.parent),
            stdout=subprocess.DEVNULL,
        )
        return out_file.read_text("utf-8")
