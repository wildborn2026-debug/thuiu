import asyncio
import io
import logging
import os
import random

from pyrogram import Client
from pyrogram.errors import FloodWait
from pathlib import Path
from pyrogram.storage import FileStorage

from app import config

logger = logging.getLogger("userbot_pool")

_clients: list[Client] = []
_op_semaphore: asyncio.Semaphore | None = None

SESSIONS_DIR = os.path.abspath("sessions")


async def _session_string_to_file(session_string: str, session_path: str, api_id: int):
    temp = Client(
        name=session_path,
        api_id=api_id,
        api_hash=config.API_HASH,
        session_string=session_string,
    )
    await temp.start()
    mem = temp.storage
    file_storage = FileStorage(name=os.path.basename(session_path), workdir=Path(SESSIONS_DIR))
    await file_storage.open()
    await file_storage.dc_id(await mem.dc_id())
    await file_storage.api_id(await mem.api_id())
    await file_storage.test_mode(await mem.test_mode())
    await file_storage.auth_key(await mem.auth_key())
    await file_storage.date(await mem.date())
    await file_storage.user_id(await mem.user_id())
    await file_storage.is_bot(await mem.is_bot())
    await file_storage.save()
    await file_storage.close()
    await temp.stop()


async def _reconnect(client: Client, index: int):
    try:
        await client.stop()
    except Exception:
        pass
    try:
        await client.start()
        me = await client.get_me()
        logger.info(f"Userbot {index}: reconnected as @{me.username or me.id}")
    except Exception as ex:
        logger.error(f"Userbot {index}: reconnect failed ({ex})")


async def start():
    global _op_semaphore
    _op_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_OPS)

    os.makedirs(SESSIONS_DIR, exist_ok=True)
    logger.info(f"Sessions directory: {SESSIONS_DIR}")

    for i, session in enumerate(config.SESSION_STRINGS, start=1):
        session_path = os.path.join(SESSIONS_DIR, f"userbot_{i}")
        session_file = f"{session_path}.session"

        if not os.path.exists(session_file):
            logger.info(f"Userbot {i}: creating disk session from session_string...")
            try:
                await _session_string_to_file(session, session_path, config.API_ID)
                logger.info(f"Userbot {i}: session file created at {session_file}")
            except Exception as ex:
                logger.error(f"Userbot {i}: failed to create session file ({ex}), skipping.")
                continue

        if not os.path.exists(session_file):
            logger.error(f"Userbot {i}: session file missing, skipping.")
            continue

        client = Client(
            name=session_path,
            api_id=config.API_ID,
            api_hash=config.API_HASH,
        )
        await client.start()
        me = await client.get_me()
        logger.info(f"Userbot {i} connected as @{me.username or me.id}")

        chat = None
        if config.CHANNEL_USERNAME:
            try:
                chat = await client.get_chat(config.CHANNEL_USERNAME)
            except Exception as ex:
                logger.warning(f"Userbot {i}: username resolve failed ({ex}), trying ID.")

        if chat is None:
            try:
                chat = await client.get_chat(config.CHANNEL_ID)
            except Exception as ex:
                logger.error(f"Userbot {i}: channel peer NOT resolved ({ex}). Downloads will fail!")

        if chat is not None:
            try:
                await client.send_message(chat.id, f"🤖 Assistant {i} Online - @{me.username or me.id}")
                logger.info(f"Userbot {i}: startup message sent to {chat.title}")
            except Exception as ex:
                logger.error(f"Userbot {i}: startup message failed ({ex})")

        _clients.append(client)

    if not _clients:
        raise RuntimeError("No userbot accounts could be started.")

    logger.info(f"Userbot pool ready with {len(_clients)} account(s).")


async def stop():
    for client in _clients:
        with __import__("contextlib").suppress(Exception):
            await client.stop()


class AllAccountsFloodWaited(Exception):
    pass


async def _try_each_account(action, action_name: str):
    available = list(enumerate(_clients, start=1))
    random.shuffle(available)

    last_wait = None
    dead_clients = []

    for index, client in available:
        try:
            return await action(client)
        except FloodWait as e:
            logger.warning(f"{action_name}: FloodWait {e.value}s on userbot {index}, trying next.")
            last_wait = e.value if last_wait is None else min(last_wait, e.value)
        except OSError as e:
            logger.warning(f"{action_name}: TCP connection dead on userbot {index} ({e}), reconnecting.")
            dead_clients.append((index, client))

    if dead_clients:
        for index, client in dead_clients:
            asyncio.create_task(_reconnect(client, index))

    if last_wait is not None and last_wait <= config.MAX_FLOODWAIT_SECONDS:
        logger.warning(f"{action_name}: all accounts flood-waited, sleeping {last_wait}s then retrying once.")
        await asyncio.sleep(last_wait)
        for index, client in available:
            try:
                return await action(client)
            except (FloodWait, OSError):
                continue

    raise AllAccountsFloodWaited(f"All {len(_clients)} account(s) unavailable for {action_name}.")


async def download_from_channel(msg_id: int) -> tuple[bytes, str] | None:
    async def action(client: Client):
        msg = await client.get_messages(config.CHANNEL_ID, msg_id)
        if not msg or (not msg.audio and not msg.video):
            logger.warning(f"download_from_channel: msg_id={msg_id} not found or has no media.")
            return None
        media = msg.audio or msg.video
        mime = media.mime_type or ("audio/mpeg" if msg.audio else "video/mp4")

        buf = await client.download_media(media.file_id, in_memory=True)
        if buf is None:
            logger.warning(f"download_from_channel: msg_id={msg_id} download_media returned None.")
            return None

        result_bytes = buf.getvalue() if hasattr(buf, "getvalue") else bytes(buf)

        if not result_bytes:
            logger.warning(f"download_from_channel: msg_id={msg_id} empty bytes after extraction.")
            return None

        logger.info(f"download_from_channel: msg_id={msg_id} downloaded {len(result_bytes)} bytes.")
        return result_bytes, mime

    async with _op_semaphore:
        try:
            return await _try_each_account(action, "download_from_channel")
        except AllAccountsFloodWaited:
            logger.error("download_from_channel: all accounts unavailable, returning None.")
            return None


async def upload_to_channel(file_bytes: bytes, file_name: str, video_id: str, is_video: bool) -> int | None:
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
        try:
            return await _try_each_account(action, "upload_to_channel")
        except AllAccountsFloodWaited:
            logger.error("upload_to_channel: all accounts unavailable, upload skipped.")
            return None
