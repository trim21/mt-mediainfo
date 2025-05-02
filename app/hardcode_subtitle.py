import subprocess
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import orjson
import PIL.Image
import regex
from rapidocr_onnxruntime import RapidOCR
from sslog import logger

from app.utils import must_find_executable, must_run_command


class Point(NamedTuple):
    x: int  # 水平方向向右
    y: int  # 垂直方向向下


pattern_chinese = regex.compile(r"\p{script=Han}")

ffprobe: str = must_find_executable("ffprobe")
logger.info("using ffprobe at {}", ffprobe)

ffmpeg: str = must_find_executable("ffmpeg")
logger.info("using ffmpeg at {}", ffmpeg)


def get_video_duration(video_file: Path) -> int:
    p = must_run_command(
        ffprobe,
        [
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            str(video_file),
        ],
        capture_output=True,
        check=False,
    )
    if p.returncode:
        print(p.stdout)
        print(p.stderr)
        raise Exception("failed to get video info")

    probe = orjson.loads(p.stdout)

    return int(orjson.loads(probe["format"]["duration"]))


def generate_images(
    video_file: Path,
    tmpdir: Path,
    image_format: str = "png",
    count: int = 3,
) -> list[Path]:
    temp = tmpdir.joinpath("images")
    temp.mkdir(exist_ok=True, parents=True)
    results = []
    duration = get_video_duration(video_file)

    # long enough
    if duration > 30 * 60:
        start = 10 * 60
        step = (duration - start * 2) // count
    else:
        start = 30
        step = (duration - 60) // count

    for i in range(count):
        seek = start + step * i - 5
        seek = seek + 5
        logger.info("screenshot from {} at {}", video_file.name, timedelta(seconds=seek))
        raw_image_file = temp.joinpath(f"{i}.raw.{image_format}")
        must_run_command(
            ffmpeg,
            [
                "-y",
                "-ss",
                str(seek),
                "-i",
                str(video_file),
                "-update",
                "1",
                "-loglevel",
                "debug",
                "-frames:v",
                "1",
                # "-compression_level",
                # "50",
                str(raw_image_file),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )

        results.append(raw_image_file)

    return results


def check_hardcode_chinese_subtitle(video_file: Path) -> bool:
    engine = RapidOCR()

    with tempfile.TemporaryDirectory(prefix="mt-") as tempdir:
        image_files = generate_images(video_file, Path(tempdir))

        for file in image_files:
            with PIL.Image.open(file) as img:
                size = Point(*img.size)

            result, _ = engine(file)
            if not result:
                continue
            for points, s, _ in result:
                points = [Point(x, y) for x, y in points]
                if points[0].y <= size.y / 2:
                    continue
                if len(pattern_chinese.sub("", s)) / len(s) < 0.5:
                    return True

    return False
