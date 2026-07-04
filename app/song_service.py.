import logging

from app import database, userbot_pool

logger = logging.getLogger("song_service")


async def get_song(video_id: str, video: bool = False) -> tuple[bytes, str] | None:
    field = "v" if video else "a"

    doc = await database.get_song(video_id)
    if doc and doc.get(field):
        msg_id = doc[field]
        result = await userbot_pool.download_from_channel(msg_id)
        if result:
            logger.info(f"{video_id}: served from channel cache (msg_id={msg_id})")
            return result
        logger.warning(f"{video_id}: cached msg_id {msg_id} could not be fetched.")

    return None
