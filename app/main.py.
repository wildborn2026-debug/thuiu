"""
FastAPI app. Exposes a /download endpoint compatible with how the bot
already calls the Shruti API (same query params: url, type, api_key),
so switching the bot over is a one-line change.
"""
import logging

from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.responses import Response

from app import config, database, userbot_pool, song_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("api")

app = FastAPI(title="Music Cache API")


@app.on_event("startup")
async def startup():
    config.validate()
    database.connect()
    await userbot_pool.start()
    logger.info("API ready.")


@app.on_event("shutdown")
async def shutdown():
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
    result = await song_service.get_song(url, video=is_video)

    if not result:
        raise HTTPException(status_code=500, detail="Download failed")

    file_bytes, mime = result
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
