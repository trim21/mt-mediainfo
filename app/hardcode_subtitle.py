import os
import platform
import subprocess
import tempfile
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import orjson
import PIL.Image
import regex
from rapidocr import EngineType, RapidOCR
from sslog import logger

from app.utils import must_run_command


class Point(NamedTuple):
    x: int  # 水平方向向右
    y: int  # 垂直方向向下


pattern_chinese = regex.compile(r"\p{script=Han}")

_ocr_engine: RapidOCR | None = None


def _is_intel_cpu() -> bool:
    system = platform.system()
    if system == "Linux":
        try:
            with open("/proc/cpuinfo", encoding="utf8") as f:
                if "GenuineIntel" in f.read():
                    return True
        except Exception:
            logger.debug("failed to read /proc/cpuinfo", exc_info=True)
    elif system == "Windows":
        ident = os.environ.get("PROCESSOR_IDENTIFIER", "") or platform.processor() or ""
        if "Intel" in ident:
            return True
    return False


def _create_ocr_engine() -> RapidOCR:
    if _is_intel_cpu():
        logger.info("Intel CPU detected, using OpenVINO backend")
        return RapidOCR(
            params={
                "Det.engine_type": EngineType.OPENVINO,
                "Cls.engine_type": EngineType.OPENVINO,
                "Rec.engine_type": EngineType.OPENVINO,
            }
        )
    logger.info("Using ONNX Runtime backend")
    return RapidOCR()


def _get_ocr_engine() -> RapidOCR:
    global _ocr_engine
    if _ocr_engine is None:
        _ocr_engine = _create_ocr_engine()
    return _ocr_engine


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
        seek = start + step * i
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

            result = _get_ocr_engine()(str(file))
            if not result.txts:
                continue
            for i in range(len(result.txts)):
                s = result.txts[i]
                if not s:
                    continue
                y0 = int(result.boxes[i][0][1])
                if y0 <= size.y / 2:
                    continue
                chinese_ratio = len(pattern_chinese.findall(s)) / len(s)
                if chinese_ratio > 0.5:
                    return True

    return False
