import subprocess
import tempfile
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import orjson
import PIL.Image
import regex
from rapidocr_onnxruntime import RapidOCR
from sslog import logger

from app.utils import must_run_command


class Point(NamedTuple):
    x: int  # 水平方向向右
    y: int  # 垂直方向向下


pattern_chinese = regex.compile(r"\p{script=Han}")

ocr_engine = RapidOCR()


def get_video_duration(ffprobe_bin: str, video_file: Path) -> int:
    p = must_run_command(
        ffprobe_bin,
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
    )

    probe = orjson.loads(p.stdout)

    return int(orjson.loads(probe["format"]["duration"]))


def generate_images(
    ffmpeg_bin: str,
    ffprobe_bin: str,
    video_file: Path,
    tmpdir: Path,
    image_format: str = "png",
    count: int = 3,
) -> Generator[Path]:
    temp = tmpdir.joinpath("images")
    temp.mkdir(exist_ok=True, parents=True)
    duration = get_video_duration(ffprobe_bin, video_file)

    # long enough
    if duration > 20 * 60:
        start = 5 * 60
        step = (duration - start * 2) // count
    else:
        start = 30
        step = (duration - 60) // count

    for i in range(count):
        seek = start + step * i - 5
        seek = seek + 5
        logger.info("screenshot from {} at {}", video_file.name, timedelta(seconds=seek))
        image_file = temp.joinpath(f"{i}.{image_format}")
        must_run_command(
            ffmpeg_bin,
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
                str(image_file),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if image_file.exists():
            yield image_file


def check_hardcode_chinese_subtitle(
    ffprobe_bin: str,
    ffmpeg_bin: str,
    video_file: Path,
) -> bool:
    with tempfile.TemporaryDirectory(prefix="mt-") as tempdir:
        for file in generate_images(ffmpeg_bin, ffprobe_bin, video_file, Path(tempdir), count=10):
            with PIL.Image.open(file) as img:
                size = Point(*img.size)

            result, _ = ocr_engine(file)
            if not result:
                continue
            for points, s, _ in result:
                points = [Point(int(x), int(y)) for x, y in points]
                if points[0].y <= size.y / 2:
                    continue

                chinese_ratio = len(pattern_chinese.findall(s)) / len(s)
                if chinese_ratio > 0.5:
                    return True

    return False
