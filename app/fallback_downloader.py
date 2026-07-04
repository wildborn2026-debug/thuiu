"""
fallback_downloader.py

Runs inside the same FastAPI process as the rest of the Music Cache API.
Whenever song_service.py can't find a video in MongoDB, it calls enqueue()
here instead of just failing. A single background worker then tries external
YouTube-download APIs for that video, one at a time, in order:

    1. NexGen
    2. Shruti
    3. Ritesh

(Devil is deliberately not in this chain -- Devil IS this Music Cache API,
so it can't be its own fallback.)

On success the file is saved as DOWNLOAD_DIR/<video_id>.mp3 (audio) or
DOWNLOAD_DIR/<video_id>.mp4 (video) -- the exact naming migrate.py already
expects, so nothing else needs to change for the daily manual migration to
pick it up.

This module never touches MongoDB or the Telegram channel. That stays
migrate.py's job.

To add another provider later: write a `_try_<name>(session, video_id,
media_type, file_path, timeout_sec) -> bool` function following the same
shape as the ones below, and add it to _PROVIDERS.
"""
import asyncio
import logging
import os
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("fallback_downloader")

# ─── Config (self-contained -- no changes to app/config.py needed) ─────────
NEXGEN_API_URL = os.environ.get("NEXGEN_API_URL", "https://pvtz.nexgenbots.xyz").rstrip("/")
NEXGEN_API_KEY = os.environ.get("NEXGEN_API_KEY", "")

SHRUTI_API_URL = os.environ.get("SHRUTI_API_URL", "https://api.shrutibots.site").rstrip("/")
SHRUTI_API_KEY = os.environ.get("SHRUTI_API_KEY", "ShrutiBotsrZH5W7D4ijdbzufb3utZ")

RITESH_API_URL = os.environ.get("RITESH_API_URL", "https://web.riteshyt.in").rstrip("/")
RITESH_API_KEY = os.environ.get("RITESH_API_KEY", "ritesh_free_ca33fedb4749ba9ed138321a")

DOWNLOAD_DIR = os.environ.get("FALLBACK_DOWNLOAD_DIR", "downloads")

NEXGEN_POLL_ATTEMPTS = 15
NEXGEN_POLL_INTERVAL = 4
STATUS_TIMEOUT = 30
AUDIO_TIMEOUT = 300
VIDEO_TIMEOUT = 600
CHUNK_SIZE = 131072

_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_in_flight: set[tuple[str, str]] = set()


def _file_path(video_id: str, media_type: str) -> Path:
    ext = "mp4" if media_type == "video" else "mp3"
    return Path(DOWNLOAD_DIR) / f"{video_id}.{ext}"


def _already_downloaded(video_id: str, media_type: str) -> bool:
    path = _file_path(video_id, media_type)
    return path.exists() and path.stat().st_size > 0


async def _try_nexgen(session: aiohttp.ClientSession, video_id: str, media_type: str, file_path: Path, timeout_sec: int) -> bool:
    """Status-poll then stream-download, matching the existing bot's nexgen flow."""
    if not (NEXGEN_API_URL and NEXGEN_API_KEY):
        return False

    endpoint = "song" if media_type == "audio" else "video"
    status_url = f"{NEXGEN_API_URL}/{endpoint}/{video_id}?api={NEXGEN_API_KEY}"
    stream_url = f"{NEXGEN_API_URL}/stream/{video_id}?api={NEXGEN_API_KEY}"

    try:
        for _ in range(NEXGEN_POLL_ATTEMPTS):
            async with session.get(status_url, timeout=aiohttp.ClientTimeout(total=STATUS_TIMEOUT)) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
                status = data.get("status")

            if status == "done":
                dl_link = data.get("link", stream_url)
                async with session.get(dl_link, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as dl_resp:
                    if dl_resp.status != 200:
                        return False
                    with open(file_path, "wb") as f:
                        async for chunk in dl_resp.content.iter_chunked(CHUNK_SIZE):
                            f.write(chunk)
                return file_path.exists() and file_path.stat().st_size > 0

            elif status == "downloading":
                await asyncio.sleep(NEXGEN_POLL_INTERVAL)
                continue
            else:
                return False
    except Exception as ex:
        logger.warning(f"{video_id}: nexgen failed ({ex})")
        return False

    return False


async def _try_shruti(session: aiohttp.ClientSession, video_id: str, media_type: str, file_path: Path, timeout_sec: int) -> bool:
    """Single GET, stream straight to disk -- matches the existing bot's shruti flow."""
    if not (SHRUTI_API_URL and SHRUTI_API_KEY):
        return False

    try:
        async with session.get(
            f"{SHRUTI_API_URL}/download",
            params={"url": video_id, "type": media_type, "api_key": SHRUTI_API_KEY},
            timeout=aiohttp.ClientTimeout(total=timeout_sec),
        ) as resp:
            if resp.status != 200:
                logger.warning(f"{video_id}: shruti failed (HTTP {resp.status})")
                return False
            with open(file_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    f.write(chunk)
        return file_path.exists() and file_path.stat().st_size > 0
    except asyncio.TimeoutError:
        logger.warning(f"{video_id}: shruti timeout")
        return False
    except Exception as ex:
        logger.warning(f"{video_id}: shruti failed ({ex})")
        return False


async def _try_ritesh(session: aiohttp.ClientSession, video_id: str, media_type: str, file_path: Path, timeout_sec: int) -> bool:
    """Single GET to a path-based URL, stream straight to disk -- matches the existing bot's ritesh flow."""
    if not (RITESH_API_URL and RITESH_API_KEY):
        return False

    ext = "mp4" if media_type == "video" else "mp3"
    url = f"{RITESH_API_URL}/downloads/{RITESH_API_KEY}/youtube.com/{video_id}.{ext}"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
            if resp.status != 200:
                logger.warning(f"{video_id}: ritesh failed (HTTP {resp.status})")
                return False
            with open(file_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    f.write(chunk)
        return file_path.exists() and file_path.stat().st_size > 0
    except asyncio.TimeoutError:
        logger.warning(f"{video_id}: ritesh timeout")
        return False
    except Exception as ex:
        logger.warning(f"{video_id}: ritesh failed ({ex})")
        return False


# Tried in this order for every job. Append another (name, function) pair
# here to plug in a 4th provider -- nothing else in this file needs to change.
_PROVIDERS = [
    ("nexgen", _try_nexgen),
    ("shruti", _try_shruti),
    ("ritesh", _try_ritesh),
]


async def _download_one(video_id: str, media_type: str):
    if _already_downloaded(video_id, media_type):
        logger.info(f"{video_id}: already on disk, skipping fallback download")
        return

    Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    file_path = _file_path(video_id, media_type)
    timeout_sec = VIDEO_TIMEOUT if media_type == "video" else AUDIO_TIMEOUT

    async with aiohttp.ClientSession() as session:
        for name, provider in _PROVIDERS:
            ok = await provider(session, video_id, media_type, file_path, timeout_sec)
            if ok:
                logger.info(f"{video_id}: downloaded via {name} -> {file_path.name}")
                return

    if file_path.exists():
        file_path.unlink(missing_ok=True)
    logger.warning(f"{video_id}: all fallback apis failed, skipping")


async def _worker():
    while True:
        video_id, media_type = await _queue.get()
        try:
            await _download_one(video_id, media_type)
        except Exception as ex:
            logger.error(f"{video_id}: unexpected error in fallback worker ({ex})")
        finally:
            _in_flight.discard((video_id, media_type))
            _queue.task_done()


def enqueue(video_id: str, video: bool):
    """
    Non-blocking -- call this from song_service.py the moment a video isn't
    found in Mongo. Safe to call repeatedly for the same video: duplicates
    are dropped if already queued/downloading, or already sitting on disk
    from an earlier request today.
    """
    media_type = "video" if video else "audio"
    key = (video_id, media_type)

    if key in _in_flight or _already_downloaded(video_id, media_type):
        return

    _in_flight.add(key)
    _queue.put_nowait(key)
    logger.info(f"{video_id}: queued for fallback download ({_queue.qsize()} pending)")


async def start():
    global _queue, _worker_task
    _queue = asyncio.Queue()
    _worker_task = asyncio.create_task(_worker())
    logger.info("Fallback downloader worker started.")


async def stop():
    if _worker_task:
        _worker_task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError):
            await _worker_task
