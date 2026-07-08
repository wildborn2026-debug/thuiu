import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("local_cache")

LOCAL_CACHE_DIR = os.environ.get("LOCAL_CACHE_DIR", "downloaded")
LOCAL_CACHE_MAX_BYTES = int(os.environ.get("LOCAL_CACHE_MAX_BYTES", 60 * 1024**3))

_current_bytes = 0


def _file_path(video_id: str, video: bool) -> Path:
    ext = "mp4" if video else "mp3"
    return Path(LOCAL_CACHE_DIR) / f"{video_id}.{ext}"


def _mime_for(video: bool) -> str:
    return "video/mp4" if video else "audio/mpeg"


async def get(video_id: str, video: bool) -> tuple[bytes, str] | None:
    path = _file_path(video_id, video)
    if not path.exists():
        return None

    try:
        with open(path, "rb") as f:
            data = f.read()
    except Exception as ex:
        logger.error(f"{video_id}: local cache read failed ({ex})")
        return None

    if not data:
        return None

    logger.info(f"{video_id}: served from local cache")
    return data, _mime_for(video)


def _wipe():
    global _current_bytes
    removed = 0
    for f in Path(LOCAL_CACHE_DIR).iterdir():
        if f.is_file():
            try:
                f.unlink()
                removed += 1
            except Exception as ex:
                logger.error(f"local cache wipe: failed to remove {f.name} ({ex})")
    _current_bytes = 0
    logger.info(f"local cache full, wiped {removed} file(s), starting fresh")


async def put(video_id: str, video: bool, data: bytes):
    global _current_bytes

    try:
        Path(LOCAL_CACHE_DIR).mkdir(parents=True, exist_ok=True)
        path = _file_path(video_id, video)
        tmp_path = Path(f"{path}.part")

        with open(tmp_path, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)

        _current_bytes += len(data)
        logger.info(f"{video_id}: added to local cache ({_current_bytes / 1024**3:.2f}GB / {LOCAL_CACHE_MAX_BYTES / 1024**3:.0f}GB)")

        if _current_bytes >= LOCAL_CACHE_MAX_BYTES:
            _wipe()
    except Exception as ex:
        logger.error(f"{video_id}: local cache write failed ({ex})")


async def start():
    global _current_bytes
    Path(LOCAL_CACHE_DIR).mkdir(parents=True, exist_ok=True)
    _current_bytes = sum(
        f.stat().st_size for f in Path(LOCAL_CACHE_DIR).iterdir() if f.is_file()
    )
    logger.info(f"Local cache ready: {_current_bytes / 1024**3:.2f}GB already on disk")
