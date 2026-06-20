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
            stderr=subprocess.PIPE,
        )
        return out_file.read_text("utf-8")


def extract_bdinfo_from_dir(bdinfocli_bin: str, dir_path: Path) -> str:
    """Run BDInfoCLI on a Blu-ray directory and return the report text."""
    with tempfile.TemporaryDirectory(prefix="bdinfo-") as tempdir:
        bdmv_dir = dir_path / "BDMV"
        if not bdmv_dir.is_dir():
            raise FileNotFoundError(f"BDMV directory not found at {dir_path}")
        must_run_command(
            bdinfocli_bin,
            ["-w", str(dir_path), tempdir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            # BDInfo prompts "Continue scanning? (y/n):" on playlist scan errors.
            # Provide enough "y\n" answers to cover all possible playlists (max 999).
            input=b"y\n" * 1000,
        )
        parts: list[str] = []
        report_dir = Path(tempdir)
        for report_file in sorted(report_dir.iterdir()):
            if report_file.is_file():
                parts.append(report_file.read_text("utf-8"))
        return "\n".join(parts)
