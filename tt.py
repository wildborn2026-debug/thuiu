"""
Migration script: dost ke public Telegram channel se apne DB-channel mein
songs/videos copy karta hai, aur apne MongoDB mein unka msg_id save karta hai.

CHALANE KA TARIKA (VPS pe):
    1. Neeche wala CONFIG block edit karo (apni values daalo).
    2. python3 migrate_friend_channel.py
    3. Script puchega: "Type 'test' or 'run': "
         - test -> sirf info dikhayega (total messages, sabse pehla msg_id),
                   kuch copy NAHI karega.
         - run  -> asli migration shuru karega, sabse pehle (oldest) message
                   se shuru karke sequentially aage badhega.

RESUME:
    Agar script beech me ruk jaye (Ctrl+C, crash, VPS restart), dobara
    "run" karne par khud hi progress.json se last processed message ke
    baad se shuru ho jayega -- shuru se dobara nahi chalega.

LOGS:
    - Terminal me har message ke baad running counters print honge:
      Successful: N   Unsuccessful: N
    - progress.json me state save hota hai (resume ke liye)
    - skipped_log.txt me har skip/fail ka reason likha jata hai (review ke liye)

YOUTUBE VALIDATION:
    - Format check (11 char, valid charset) HAR ID pe hota hai.
    - Real-exists check (YouTube oEmbed se) SIRF doubtful IDs pe hota hai --
      matlab jinke caption/file_name mein junk suffix tha (jaise
      '%28mp3j.cc%29-2647...'), ya jinki ID file_name se fallback karke
      mili (caption se nahi). Clean IDs (seedha caption + standard
      extension) is check ko skip kar dete hain -- taaki YouTube ko bhi
      flood na karein, jaisa ki koi bhi service bahut fast hit karne par
      block/throttle kar sakti hai.
"""
import asyncio
import json
import logging
import re
import sys
from pathlib import Path

import aiohttp
from pyrogram import Client
from pyrogram.enums import MessageMediaType
from pyrogram.errors import FloodWait

from motor.motor_asyncio import AsyncIOMotorClient

SESSION_STRING = "PASTE_YOUR_USERBOT_SESSION_STRING_HERE"
API_ID = 0
API_HASH = "PASTE_YOUR_API_HASH_HERE"

MONGO_URI = "PASTE_YOUR_MONGO_URI_HERE"
DB_NAME = "musicbot"
COLLECTION = "songs"

MY_CHANNEL_ID = 0

FRIEND_CHANNEL_USERNAME = "FALLEN_API_USERNAME_YAHAN"   # bina @ ke, jaisa "fallenapi"

SLEEP_BETWEEN_MESSAGES = 1.0     # tumne bola tha "1 sec ruke fir next"
FLOOD_EXTRA_BUFFER = 5           # FloodWait.value + itna extra buffer
MAX_CONSECUTIVE_FLOODS = 6       # itni baar lagatar flood aaye to gap badhao
YOUTUBE_CHECK_SLEEP = 0.5        # har oembed call ke baad itna ruko (apna khud ka rate-limit)

PROGRESS_FILE = "progress.json"
SKIPPED_LOG_FILE = "skipped_log.txt"

# ═══════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")

VALID_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}")


# ═══════════════════════════════════════════════════════════════════════
# PROGRESS STATE (resume support)
# ═══════════════════════════════════════════════════════════════════════

def load_progress() -> dict:
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {
        "last_message_id": 0,   # 0 = abhi tak kuch process nahi hua
        "total_copied": 0,
        "total_skipped": 0,
        "total_failed": 0,
    }


def save_progress(state: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(state, f, indent=2)


def log_skip(msg_id: int, reason: str, detail: str = ""):
    with open(SKIPPED_LOG_FILE, "a") as f:
        f.write(f"msg_id={msg_id} | {reason} | {detail}\n")


def print_counters(state: dict):
    log.info(
        f"Successful: {state['total_copied']}   "
        f"Unsuccessful: {state['total_skipped'] + state['total_failed']}"
    )


# ═══════════════════════════════════════════════════════════════════════
# VIDEO ID EXTRACTION
# ═══════════════════════════════════════════════════════════════════════

STANDARD_EXTENSIONS = (".mp3", ".mp4", ".m4a", ".webm", ".opus")


def extract_video_id(caption: str | None, file_name: str | None) -> tuple[str | None, bool]:
    """
    Pehle 11 valid characters (A-Za-z0-9_-) nikalta hai, chahe caption ya
    filename ke aage koi bhi junk suffix ho (jaise '.mp3', '%28mp3j.cc%29-2647...').
    YouTube video ID hamesha exactly 11 characters ki hoti hai.

    Returns (video_id, is_clean):
      - is_clean=True  -> ID caption se mili, aur uske baad sirf ek standard
                          extension tha (ya kuch nahi) -- koi junk suffix nahi.
                          Aise IDs pe hum YouTube ke saamne validate NAHI
                          karte (bharosa kar lete hain), taaki YouTube ko
                          bhi flood na karein.
      - is_clean=False -> ID file_name se fallback karke mili, YA caption/
                          file_name mein 11 chars ke baad koi non-standard
                          junk tha. Ye "doubtful" hai -- YouTube oEmbed se
                          validate hoga.
    """
    # Pehle caption try karo -- agar yahan se saaf ID milti hai to trusted hai
    if caption:
        stem = caption.split("%")[0]
        match = VALID_ID_RE.match(stem)
        if match:
            video_id = match.group(0)
            remainder = caption[len(video_id):]
            is_clean = (remainder == "") or (remainder.lower() in STANDARD_EXTENSIONS)
            return video_id, is_clean

    # Caption se nahi mili -- file_name pe fallback (hamesha doubtful maana jayega,
    # kyunki caption absent hone ka matlab hai upload flow standard nahi tha)
    if file_name:
        stem = file_name.split("%")[0]
        match = VALID_ID_RE.match(stem)
        if match:
            return match.group(0), False

    return None, False


def is_valid_youtube_id(video_id: str) -> bool:
    """Sirf FORMAT check -- 11 chars, valid charset. Ye YouTube pe real
    hone ki guarantee nahi deta, sirf shape check hai."""
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id))


async def youtube_id_exists(session: aiohttp.ClientSession, video_id: str) -> bool:
    """
    YouTube ke oEmbed endpoint se check karta hai ki video abhi bhi
    exist karti hai (public hai, deleted nahi hai, sahi ID hai).
    Koi API key nahi chahiye. Sirf DOUBTFUL IDs ke liye call hota hai
    (clean IDs ke liye skip -- taaki YouTube ko flood na karein).
    """
    url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return resp.status == 200
    except Exception as ex:
        log.warning(f"{video_id}: youtube oembed check fail ({ex}) -- benefit of doubt de raha hu")
        return True

def classify_media(message) -> tuple[str | None, object | None]:
    """
    Returns (field, media_obj) where field is "a" (audio) or "v" (video),
    ya (None, None) agar ye message kaam ka media nahi hai.

    Extension (mp3/mp4/m4a/webm/opus) sirf reference ke liye hai -- asli
    decision Telegram ke media_type aur width/height se hoti hai, kyunki
    .webm jaisi files audio-only bhi ho sakti hain (black-screen-but-plays
    wala case jo tumne khud verify kiya tha Google Photos mein).
    """
    if message.media == MessageMediaType.AUDIO and message.audio:
        return "a", message.audio

    if message.media == MessageMediaType.VIDEO and message.video:
        video = message.video
        if not video.width or not video.height:
            return "a", video
        return "v", video

    if message.media == MessageMediaType.DOCUMENT and message.document:
        doc = message.document
        mime = (doc.mime_type or "").lower()
        if mime.startswith("audio/"):
            return "a", doc
        if mime.startswith("video/"):
            # document ke andar width/height nahi hota seedha -- mime hi
            # bharosa karne layak signal hai is case mein
            return "v", doc
        return None, None  # ambiguous document -- skip

    return None, None  # koi media nahi (GIF, photo, text, link, sticker)


def is_broken_or_suspicious(media_obj) -> bool:
    """file_size ya duration 0/missing -- corrupt ya placeholder file."""
    file_size = getattr(media_obj, "file_size", None)
    duration = getattr(media_obj, "duration", None)
    if not file_size or file_size <= 0:
        return True
    if duration is not None and duration <= 0:
        return True
    return False

async def run_test_mode(client: Client, friend_chat_id: int):
    print("\n" + "=" * 50)
    print("TEST MODE -- kuch copy nahi hoga, sirf info")
    print("=" * 50)

    newest_id = None
    async for message in client.get_chat_history(friend_chat_id, limit=1):
        newest_id = message.id

    oldest_id = None
    async for message in client.get_chat_history(friend_chat_id, offset_id=1, limit=1, offset=-1):
        oldest_id = message.id

    print(f"Sabse naya (newest) msg_id: {newest_id}")
    print(f"Sabse pehla (oldest) msg_id: {oldest_id}")
    if newest_id and oldest_id:
        print(f"Approximate ID range: {oldest_id} - {newest_id} "
              f"(asli total isse kam hoga agar beech mein deleted messages hain)")
    print("=" * 50 + "\n")


async def run_migration(client: Client, friend_chat_id: int, col, state: dict, http_session: aiohttp.ClientSession):
    print("\n" + "=" * 50)
    print("RUN MODE -- migration shuru ho rahi hai")
    print("=" * 50 + "\n")

    consecutive_floods = 0

    resume_from = state["last_message_id"]
    log.info(f"Resume point: {resume_from} (0 = shuru se)")
    log.info("Oldest-first streaming shuru ho rahi hai...\n")

    try:
        async for message in client.get_chat_history(
            friend_chat_id,
            offset_id=resume_from + 1 if resume_from else 1,
            offset=-1,
        ):
            msg_id = message.id

            if message is None or message.empty:
                state["last_message_id"] = msg_id
                continue

            # 1. Media check
            field, media_obj = classify_media(message)
            if field is None:
                # GIF, photo, text, link, sticker, ya ambiguous document -- skip
                state["last_message_id"] = msg_id
                continue

            # 2 & 3. video_id extract + format validate
            caption = message.caption.strip() if message.caption else None
            file_name = getattr(media_obj, "file_name", None)
            video_id, is_clean = extract_video_id(caption, file_name)

            if not video_id or not is_valid_youtube_id(video_id):
                state["total_skipped"] += 1
                log_skip(msg_id, "no_valid_id", f"caption={caption!r} file_name={file_name!r}")
                state["last_message_id"] = msg_id
                save_progress(state)
                print_counters(state)
                continue
            if not is_clean:
                exists = await youtube_id_exists(http_session, video_id)
                await asyncio.sleep(YOUTUBE_CHECK_SLEEP)
                if not exists:
                    state["total_skipped"] += 1
                    log_skip(msg_id, "youtube_id_not_found", f"video_id={video_id}")
                    state["last_message_id"] = msg_id
                    save_progress(state)
                    print_counters(state)
                    continue

            # 4. Apne MongoDB mein dedup check
            doc = await col.find_one({"_id": video_id})
            if doc and doc.get(field) is not None:
                state["total_skipped"] += 1
                log_skip(msg_id, "already_in_db", f"video_id={video_id} field={field}")
                state["last_message_id"] = msg_id
                save_progress(state)
                print_counters(state)
                continue

            # 5. Broken/suspicious check
            if is_broken_or_suspicious(media_obj):
                state["total_skipped"] += 1
                log_skip(msg_id, "suspicious_file", f"video_id={video_id}")
                state["last_message_id"] = msg_id
                save_progress(state)
                print_counters(state)
                continue

            # 6. Copy karo (forward tag ke bina, apna clean caption)
            flood_retries = 0
            while True:
                try:
                    copied = await client.copy_message(
                        chat_id=MY_CHANNEL_ID,
                        from_chat_id=friend_chat_id,
                        message_id=msg_id,
                        caption=video_id,
                    )
                    new_msg_id = copied.id

                    await col.update_one(
                        {"_id": video_id},
                        {"$set": {field: new_msg_id}},
                        upsert=True,
                    )

                    state["total_copied"] += 1
                    consecutive_floods = 0
                    log.info(f"msg_id={msg_id}: COPIED video_id={video_id} field={field} -> new_msg_id={new_msg_id}")
                    break

                except FloodWait as e:
                    flood_retries += 1
                    consecutive_floods += 1
                    wait_time = e.value + FLOOD_EXTRA_BUFFER

                    if consecutive_floods >= MAX_CONSECUTIVE_FLOODS:
                        # Baar baar flood aa raha hai -- account safe rakhne ke
                        # liye is baar zyada lamba ruko
                        wait_time += 60
                        log.warning(
                            f"msg_id={msg_id}: {consecutive_floods} baar lagatar flood -- "
                            f"extra cooldown, total {wait_time}s"
                        )

                    log.warning(f"msg_id={msg_id}: FloodWait #{flood_retries}, {wait_time}s so raha hu")
                    await asyncio.sleep(wait_time)
                    continue

                except Exception as ex:
                    state["total_failed"] += 1
                    log.error(f"msg_id={msg_id}: copy FAILED ({ex})")
                    log_skip(msg_id, "copy_error", str(ex))
                    break

            state["last_message_id"] = msg_id
            save_progress(state)
            print_counters(state)

            await asyncio.sleep(SLEEP_BETWEEN_MESSAGES)

    except FloodWait as e:
        log.warning(f"get_chat_history flood-waited {e.value}s. Progress save ho gaya -- "
                     f"dobara 'run' karke resume karo.")
        save_progress(state)
        return

    print("\n" + "=" * 50)
    print("MIGRATION COMPLETE")
    print(f"   Copied   : {state['total_copied']}")
    print(f"   Skipped  : {state['total_skipped']}")
    print(f"   Failed   : {state['total_failed']}")
    print("=" * 50)


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

async def main():
    mode = input("Type 'test' or 'run': ").strip().lower()
    if mode not in ("test", "run"):
        print("Invalid input. 'test' ya 'run' type karo.")
        sys.exit(1)

    client = Client(
        "migrate_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STRING,
    )
    await client.start()
    log.info("Userbot connected.")

    # Dost ke channel mein member ho jao agar nahi hai (public channel)
    try:
        await client.join_chat(FRIEND_CHANNEL_USERNAME)
        log.info(f"Joined @{FRIEND_CHANNEL_USERNAME}")
    except Exception as ex:
        log.info(f"Join skip/already-member ({ex})")

    friend_chat = await client.get_chat(FRIEND_CHANNEL_USERNAME)
    friend_chat_id = friend_chat.id

    if mode == "test":
        await run_test_mode(client, friend_chat_id)
        await client.stop()
        return

    # mode == "run"
    mongo = AsyncIOMotorClient(MONGO_URI)
    col = mongo[DB_NAME][COLLECTION]
    state = load_progress()

    async with aiohttp.ClientSession() as http_session:
        try:
            await run_migration(client, friend_chat_id, col, state, http_session)
        except KeyboardInterrupt:
            log.info("Ctrl+C mila -- progress save karke ruk raha hu. Dobara 'run' karne par yahi se resume hoga.")
            save_progress(state)
        finally:
            await client.stop()
            mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
