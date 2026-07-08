import logging
import time

from app import database, fallback_downloader, local_cache, userbot_pool

logger = logging.getLogger("song_service")


class SongNotCached(Exception):
    """Video isn't in MongoDB yet -- never uploaded to the channel."""


class SongFetchFailed(Exception):
    """Video IS in MongoDB, but couldn't be pulled from the Telegram channel right now."""


async def get_song(video_id: str, video: bool = False) -> tuple[bytes, str]:
    field = "v" if video else "a"
    started = time.monotonic()

    cached = await local_cache.get(video_id, video)
    if cached:
        elapsed = time.monotonic() - started
        logger.info(f"{video_id}: served from local cache ({elapsed:.2f}s)")
        return cached

    doc = await database.get_song(video_id)

    if not doc or not doc.get(field):
        try:
            fallback_downloader.enqueue(video_id, video)
            note = "queued for fallback download"
        except Exception as ex:
            logger.error(f"{video_id}: fallback enqueue failed ({ex})")
            note = "fallback enqueue FAILED"
        elapsed = time.monotonic() - started
        logger.info(f"{video_id}: not cached ({'video' if video else 'audio'}), {note} ({elapsed:.2f}s)")
        raise SongNotCached(video_id)

    msg_id = doc[field]

    try:
        result = await userbot_pool.download_from_channel(msg_id)
    except Exception as ex:
        elapsed = time.monotonic() - started
        logger.error(f"{video_id}: fetch error for msg_id={msg_id} ({ex}) ({elapsed:.2f}s)")
        raise SongFetchFailed(video_id) from ex

    elapsed = time.monotonic() - started

    if result:
        logger.info(f"{video_id}: served from channel cache (msg_id={msg_id}) ({elapsed:.2f}s)")
        await local_cache.put(video_id, video, result[0])
        return result

    logger.warning(f"{video_id}: cached msg_id {msg_id} could not be fetched ({elapsed:.2f}s)")
    raise SongFetchFailed(video_id)
