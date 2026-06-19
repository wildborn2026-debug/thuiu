"""
Manages 1-3 Pyrogram userbot accounts. Any operation (download from
channel, upload to channel) is tried on each account in turn — if one
hits a FloodWait, the next account is tried automatically. Works the
same whether you configured 1, 2, or 3 accounts.
"""
import asyncio
import io
import logging

from pyrogram import Client
from pyrogram.errors import FloodWait

from app import config

logger = logging.getLogger("userbot_pool")

_clients: list[Client] = []
_op_semaphore: asyncio.Semaphore | None = None


async def start():
    global _op_semaphore
    _op_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_OPS)

    for i, session in enumerate(config.SESSION_STRINGS, start=1):
        client = Client(
            name=f"userbot_{i}",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=session,
            in_memory=True,
        )
        await client.start()
        me = await client.get_me()
        logger.info(f"Userbot {i} connected as @{me.username or me.id}")

        # in_memory sessions start with an empty peer cache every time the
        # process restarts, so resolve_peer(CHANNEL_ID) fails until the
        # account "sees" the channel again. Force that resolution now.
        try:
            chat = await client.get_chat(config.CHANNEL_ID)
            logger.info(f"Userbot {i}: channel peer resolved ({chat.title})")
        except Exception as ex:
            logger.warning(
                f"Userbot {i}: could not resolve channel by ID ({ex}); "
                f"trying CHANNEL_USERNAME fallback."
            )
            if config.CHANNEL_USERNAME:
                try:
                    chat = await client.join_chat(config.CHANNEL_USERNAME)
                except Exception:
                    chat = await client.get_chat(config.CHANNEL_USERNAME)
                logger.info(f"Userbot {i}: channel peer resolved via username ({chat.title})")
            else:
                logger.error(
                    f"Userbot {i}: channel peer NOT resolved. Set CHANNEL_USERNAME "
                    f"in .env so this can self-heal on every restart."
                )

        _clients.append(client)

    if not _clients:
        raise RuntimeError("No userbot accounts could be started.")

    logger.info(f"Userbot pool ready with {len(_clients)} account(s).")


async def stop():
    for client in _clients:
        with __import__("contextlib").suppress(Exception):
            await client.stop()


class AllAccountsFloodWaited(Exception):
    """Raised when every configured account is currently flood-waited."""
    pass


async def _try_each_account(action, action_name: str):
    """
    Runs `action(client)` against each account in order. On FloodWait,
    moves to the next account. If every account is flood-waited, waits
    out the shortest one (capped at MAX_FLOODWAIT_SECONDS) and retries once.
    """
    last_wait = None
    for client in _clients:
        try:
            return await action(client)
        except FloodWait as e:
            logger.warning(f"{action_name}: FloodWait {e.value}s on account, trying next.")
            last_wait = e.value if last_wait is None else min(last_wait, e.value)
            continue

    # Every account flood-waited — wait it out once if it's short enough.
    if last_wait is not None and last_wait <= config.MAX_FLOODWAIT_SECONDS:
        logger.warning(f"{action_name}: all accounts flood-waited, sleeping {last_wait}s then retrying once.")
        await asyncio.sleep(last_wait)
        for client in _clients:
            try:
                return await action(client)
            except FloodWait:
                continue

    raise AllAccountsFloodWaited(f"All {len(_clients)} account(s) are flood-waited for {action_name}.")


async def download_from_channel(msg_id: int) -> tuple[bytes, str] | None:
    """Downloads a previously-cached file from the channel. Returns (bytes, mime_type) or None."""
    async def action(client: Client):
        msg = await client.get_messages(config.CHANNEL_ID, msg_id)
        if not msg or (not msg.audio and not msg.video):
            logger.warning(f"download_from_channel: msg_id={msg_id} not found or has no media.")
            return None
        file_bytes = await client.download_media(msg, in_memory=True)
        if not file_bytes:
            logger.warning(f"download_from_channel: msg_id={msg_id} download_media returned empty.")
            return None
        result_bytes = bytes(file_bytes)
        logger.info(f"download_from_channel: msg_id={msg_id} downloaded {len(result_bytes)} bytes.")
        media = msg.audio or msg.video
        mime = media.mime_type or ("audio/mpeg" if msg.audio else "video/mp4")
        return result_bytes, mime

    async with _op_semaphore:
        return await _try_each_account(action, "download_from_channel")


async def upload_to_channel(file_bytes: bytes, file_name: str, video_id: str, is_video: bool) -> int:
    """Uploads file bytes to the channel, returns the new message id."""
    async def action(client: Client):
        buf = io.BytesIO(file_bytes)
        buf.name = file_name
        if is_video:
            sent = await client.send_video(
                chat_id=config.CHANNEL_ID,
                video=buf,
                caption=video_id,
            )
        else:
            sent = await client.send_audio(
                chat_id=config.CHANNEL_ID,
                audio=buf,
                caption=video_id,
            )
        return sent.id

    async with _op_semaphore:
        return await _try_each_account(action, "upload_to_channel")
