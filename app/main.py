"""
FastAPI app. Exposes a /download endpoint compatible with how the bot
already calls the Shruti API (same query params: url, type, api_key),
so switching the bot over is a one-line change.
"""
import logging

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response

from app import config, database, fallback_downloader, local_cache, song_service, userbot_pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logger = logging.getLogger("api")

app = FastAPI(title="Music Cache API")


@app.on_event("startup")
async def startup():
    config.validate()
    database.connect()
    await userbot_pool.start()
    await fallback_downloader.start()
    await local_cache.start()
    logger.info("API ready.")


@app.on_event("shutdown")
async def shutdown():
    await fallback_downloader.stop()
    await userbot_pool.stop()
    database.close()


def _check_api_key(api_key: str):
    if api_key != config.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/download")
async def download(
    url: str = Query(..., description="YouTube video ID"),
    type: str = Query("audio", pattern="^(audio|video)$"),
    api_key: str = Query(...),
):
    _check_api_key(api_key)

    is_video = type == "video"

    try:
        file_bytes, mime = await song_service.get_song(url, video=is_video)
    except song_service.SongNotCached:
        raise HTTPException(status_code=404, detail="not_cached")
    except song_service.SongFetchFailed:
        raise HTTPException(status_code=503, detail="fetch_failed")
    except Exception as ex:
        logger.error(f"{url}: unexpected error ({ex})")
        raise HTTPException(status_code=500, detail="internal_error")

    ext = "mp4" if is_video else mime.split("/")[-1].split(";")[0]

    return Response(
        content=file_bytes,
        media_type=mime,
        headers={
            "Content-Disposition": f'attachment; filename="{url}.{ext}"'
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "accounts": len(config.SESSION_STRINGS)}
