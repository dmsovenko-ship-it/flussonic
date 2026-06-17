#!/usr/bin/env python3
"""HTTP-сервер: webhook Flussonic → Telegram (с доп. обогащением через API)."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import traceback
from datetime import datetime

import aiohttp
from aiohttp import web

def _load_env(path: str = ".env") -> None:
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

_load_env()

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s",
                    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))
logger = logging.getLogger("webhook-recv")

# --- Конфигурация ----------------------------------------------------------
FLUSSONIC_URL = os.environ.get("FLUSSONIC_URL", "http://172.20.1.21").rstrip("/")
FLUSSONIC_LOGIN = os.environ.get("FLUSSONIC_LOGIN", "")
FLUSSONIC_PASS = os.environ.get("FLUSSONIC_PASSWORD", "")
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
TG_API_URL = os.environ.get("TG_API_URL", "https://api.telegram.org").rstrip("/")

TG_CHAT_ID_FACE   = os.environ.get("TG_CHAT_ID_FACE", TG_CHAT_ID)
TG_CHAT_ID_PLATE  = os.environ.get("TG_CHAT_ID_PLATE", TG_CHAT_ID)
TG_CHAT_ID_MOTION = os.environ.get("TG_CHAT_ID_MOTION", TG_CHAT_ID)

PROXY_URL = os.environ.get("PROXY_URL", "").strip()
LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8081"))
DOWNLOAD_VIDEO = os.environ.get("DOWNLOAD_VIDEO", "true").lower() not in ("false", "0", "no")
EVENT_TYPES = os.environ.get("EVENT_TYPES", "").split(",")  # пусто = все

_basic_auth_header: dict[str, str] = {}
if FLUSSONIC_PASS:
    _basic_auth_header = {"Authorization": aiohttp.BasicAuth(FLUSSONIC_LOGIN, FLUSSONIC_PASS).encode()}
_jwt_token: str = ""
_refresh_token: str = ""
_jwt_lock = asyncio.Lock()
_session: aiohttp.ClientSession | None = None
_tg_session: aiohttp.ClientSession | None = None
_sent_webhook_ids: set[str] = set()  # дедупликация по object_id/episode_id

# --- Словари перевода ----------------------------------------------------
VEHICLE_PURPOSE_RU: dict[str, str] = {
    "regular": "обычный", "emergency": "экстренный", "special": "специальный",
    "public": "общественный", "cargo": "грузовой", "taxi": "такси", "car_sharing": "каршеринг",
}
VEHICLE_EMERGENCY_RU: dict[str, str] = {
    "ambulance": "скорая помощь", "police": "полиция", "fire": "пожарная",
    "mchs": "МЧС", "emergency": "аварийная", "military": "военный",
}
FACING_RU: dict[str, str] = {"front": "перед", "rear": "зад", "left": "лево", "right": "право"}

VEHICLE_COLOR_RU: dict[str, str] = {
    "black": "чёрный", "white": "белый", "gray": "серый", "grey": "серый",
    "silver": "серебристый", "red": "красный", "blue": "синий", "green": "зелёный",
    "yellow": "жёлтый", "cyan": "голубой", "magenta": "пурпурный", "orange": "оранжевый",
    "brown": "коричневый", "purple": "фиолетовый", "pink": "розовый", "beige": "бежевый",
    "gold": "золотой", "maroon": "бордовый", "navy": "тёмно-синий", "teal": "бирюзовый",
    "violet": "лиловый", "turquoise": "бирюзовый", "indigo": "индиго", "lime": "лаймовый",
    "olive": "оливковый", "tan": "бежевый", "coral": "коралловый",
}


async def login():
    global _jwt_token, _refresh_token
    async with _jwt_lock:
        if _jwt_token:
            return _jwt_token
        url = f"{FLUSSONIC_URL}/watcher/client-api/v3/login"
        try:
            async with _session.post(url, headers=_basic_auth_header,
                                     timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _jwt_token = data.get("access_token", "")
                    _refresh_token = data.get("refresh_token", "")
                    logger.info("JWT login OK (Basic Auth)")
                    return _jwt_token
                logger.error(f"Login failed: {resp.status}")
        except Exception as e:
            logger.error(f"Login error: {e}")
        return ""


async def refresh():
    global _jwt_token, _refresh_token
    async with _jwt_lock:
        if not _refresh_token:
            return ""
        url = f"{FLUSSONIC_URL}/watcher/client-api/v3/login"
        headers = {"Authorization": f"Bearer {_refresh_token}"}
        try:
            async with _session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _jwt_token = data.get("access_token", "")
                    if new_refresh := data.get("refresh_token", ""):
                        _refresh_token = new_refresh
                    logger.info("JWT refreshed OK")
                    return _jwt_token
                logger.warning(f"Refresh failed: {resp.status}")
                _refresh_token = ""
        except Exception as e:
            logger.warning(f"Refresh error: {e}")
        return ""


def jwt_headers() -> dict:
    return {"Authorization": f"Bearer {_jwt_token}"} if _jwt_token else {}


async def api_get(path: str) -> dict | None:
    """GET-запрос к API v3 (с обработкой редиректа HTTPS→HTTP)."""
    headers = jwt_headers()
    url = f"{FLUSSONIC_URL}{path}"
    try:
        async with _session.get(url, headers=headers, allow_redirects=False,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return await resp.json()
            if resp.status == 401:
                _jwt_token = ""  # force re-login
                return None
            if resp.status in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location", "")
                if loc:
                    if loc.startswith("https://"):
                        loc = "http://" + loc[8:]
                    logger.debug(f"API redirect → {loc[:100]}")
                    async with _session.get(loc, headers=headers,
                                            timeout=aiohttp.ClientTimeout(total=10)) as r2:
                        if r2.status == 200:
                            return await r2.json()
            logger.debug(f"API {resp.status} on {url}")
    except Exception as e:
        logger.debug(f"API error on {url}: {e}")
    return None


async def enrich_event(data: dict) -> dict:
    """Обогатить webhook-событие данными из API (эпизод детально)."""
    eid = data.get("id", "")
    if not eid:
        return data
    detail = await api_get(f"/watcher/client-api/v3/episodes/{eid}")
    if detail and isinstance(detail, dict) and detail.get("episode_id"):
        # Мержим: webhook-данные + API-детали
        for key in ("matched_persons", "detections", "vehicle_model", "vehicle_color",
                     "license_plate_text", "vehicle_purpose", "vehicle_facing_side",
                     "vehicle_emergency_subtype", "license_plate_missing",
                     "preview", "frame_preview", "title", "description"):
            if key in detail and key not in data:
                data[key] = detail[key]
        data["episode_type"] = detail.get("episode_type", "")
        data["stream_obj"] = detail.get("stream", {})
    return data


def _cam_name(data: dict) -> str:
    """Человеческое имя камеры."""
    stream = data.get("stream_obj", {})
    if isinstance(stream, dict):
        return stream.get("title", stream.get("name", ""))
    return data.get("camera_id", "?")


def format_ts(raw) -> str:
    if not raw:
        return ""
    try:
        ts = raw / 1000 if isinstance(raw, (int, float)) and raw > 1e12 else raw
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return str(raw)[:19]


def extract_image(data: dict) -> bytes | None:
    """Извлечь изображение: vector (webhook) → thumbnail → preview → frame_preview."""
    # 1. vector из webhook (всегда base64 JPEG)
    vector = data.get("vector", "")
    if vector and len(vector) > 100:
        try:
            return base64.b64decode(vector)
        except Exception:
            pass

    # 2. thumbnail из detections
    detections = data.get("detections", [])
    if isinstance(detections, list) and detections:
        d = detections[0]
        if isinstance(d, dict):
            thumb = d.get("thumbnail", {})
            if isinstance(thumb, dict):
                b64 = thumb.get("data", "")
                if b64:
                    try:
                        return base64.b64decode(b64)
                    except Exception:
                        pass

    # 3. preview / frame_preview
    for fld in ("preview", "frame_preview"):
        val = data.get(fld, "")
        if val and len(val) > 100:
            try:
                return base64.b64decode(val)
            except Exception:
                pass

    return None


# --- Telegram --------------------------------------------------------------

async def tg_send_video(caption: str, video: bytes, chat_id: str = ""):
    api = f"{TG_API_URL}/bot{TG_TOKEN}"
    form = aiohttp.FormData()
    form.add_field("chat_id", chat_id or TG_CHAT_ID)
    form.add_field("caption", caption)
    form.add_field("parse_mode", "HTML")
    form.add_field("video", video, filename="event.mp4", content_type="video/mp4")
    async with _tg_session.post(f"{api}/sendVideo", data=form) as resp:
        if resp.status != 200:
            logger.error(f"TG sendVideo: {resp.status}")

async def tg_send_photo(caption: str, img: bytes, chat_id: str = ""):
    api = f"{TG_API_URL}/bot{TG_TOKEN}"
    form = aiohttp.FormData()
    form.add_field("chat_id", chat_id or TG_CHAT_ID)
    form.add_field("caption", caption)
    form.add_field("parse_mode", "HTML")
    form.add_field("photo", img, filename="snapshot.jpg")
    async with _tg_session.post(f"{api}/sendPhoto", data=form) as resp:
        if resp.status != 200:
            logger.error(f"TG sendPhoto: {resp.status}")

async def tg_send_message(text: str, chat_id: str = ""):
    api = f"{TG_API_URL}/bot{TG_TOKEN}"
    async with _tg_session.post(f"{api}/sendMessage", json={
        "chat_id": chat_id or TG_CHAT_ID, "text": text, "parse_mode": "HTML",
    }) as resp:
        if resp.status != 200:
            logger.error(f"TG sendMessage: {resp.status}")


# --- Обработка webhook -----------------------------------------------------

async def handle_webhook(request: web.Request) -> web.Response:
    try:
        raw = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")

    events = raw if isinstance(raw, list) else [raw]
    failed = False

    if not _jwt_token:
        if _refresh_token:
            await refresh()
        if not _jwt_token:
            await login()

    for data in events:
        try:
            ok = await process_event(data)
            if not ok:
                failed = True
        except Exception as e:
            logger.error(f"Event error: {e}\n{traceback.format_exc()}")
            failed = True

    return web.Response(status=500 if failed else 200, text="partial failure" if failed else "ok")


def _webhook_event_type(ev_type: str, data: dict) -> str:
    """Определить тип события из webhook-данных (face / plate / motion)."""
    if "fr" in ev_type or "face" in ev_type or data.get("episode_type") == "face":
        return "face"
    if "lp" in ev_type or "plate" in ev_type or data.get("episode_type") == "vehicle":
        return "plate"
    if data.get("detections") or data.get("matched_persons"):
        return "face"
    if data.get("license_plate_text") or data.get("vehicle_model"):
        return "plate"
    return "motion"


async def process_event(data: dict) -> bool:
    logger.info(f"Webhook: {json.dumps(data, ensure_ascii=False)[:500]}")

    # Обогащаем через API (matched_persons, vehicle_model, etc.)
    data = await enrich_event(data)

    ev_type = str(data.get("event_type", "")).lower()
    obj_class = str(data.get("object_class", "")).lower()
    cam = _cam_name(data)
    obj_id = str(data.get("object_id", ""))
    action = str(data.get("object_action", data.get("activity_type", "")))
    ts = format_ts(data.get("start_at", data.get("opened_at", 0)))

    # Фильтр по типам событий (если задан — обрабатываем только указанные)
    wtype = _webhook_event_type(ev_type, data)
    if EVENT_TYPES and EVENT_TYPES != [""] and wtype not in EVENT_TYPES:
        logger.debug(f"Webhook skip: тип {wtype} не в EVENT_TYPES={EVENT_TYPES}")
        return True

    # Дедупликация: пропускаем уже обработанные события
    dedup_key = obj_id or str(data.get("start_at", data.get("opened_at", "")))
    if dedup_key and dedup_key in _sent_webhook_ids:
        logger.debug(f"Webhook skip: дубликат {dedup_key}")
        return True
    if dedup_key:
        _sent_webhook_ids.add(dedup_key)
        if len(_sent_webhook_ids) > 10000:
            _sent_webhook_ids.clear()

    # ── FACE ────────────────────────────────────────────────────
    if "fr" in ev_type or "face" in ev_type or data.get("episode_type") == "face" or (
       ev_type == "" and (data.get("detections") or data.get("matched_persons"))):
        # Пробуем найти имя через API persons или взять из matched_persons
        name = data.get("resolved_name", "") or "Неизвестный"
        if name == "Неизвестный":
            persons = data.get("matched_persons", [])
            if persons and isinstance(persons, list) and isinstance(persons[0], dict):
                pobj = persons[0].get("person", {})
                pname = pobj.get("name", "")
                if pname and pname != "unknown":
                    name = pname
                # Если имя не найдено — пробуем API по person_id из matched_persons
                if name == "Неизвестный" and _jwt_token:
                    pid = str(pobj.get("person_id", ""))
                    if pid:
                        try:
                            person_data = await api_get(f"/watcher/client-api/v3/persons/{pid}")
                            if person_data and isinstance(person_data, dict) and person_data.get("name"):
                                name = person_data["name"]
                        except Exception:
                            pass
        if name == "Неизвестный" and obj_id:
            name = f"ID:{obj_id}"

        lines = [f"👤 {name} — {cam}"]
        if action:
            d = {"enter": "вход", "leave": "выход"}.get(action, action)
            lines.append(f"  Направление: {d}")
        if ts:
            lines.append(f"  {ts}")

    # ── PLATE / VEHICLE ──────────────────────────────────────────
    elif "lp" in ev_type or "plate" in ev_type or data.get("episode_type") == "vehicle":
        logger.info(f"Webhook skip: plate — обрабатывается poll_watcher (добавь plate в EVENT_TYPES если хочешь здесь)")
        return True
    else:
        title = data.get("title", "")
        lines = [f"🎬 Движение: {cam}"]
        if title:
            lines.append(f"  {title}")
        if ts:
            lines.append(f"  {ts}")

    caption = "\n".join(lines)

    # Выбираем чат по типу события
    chat_id = {
        "face": TG_CHAT_ID_FACE,
        "plate": TG_CHAT_ID_PLATE,
    }.get(wtype, TG_CHAT_ID_MOTION)

    # Картинка: vector → thumbnail → preview → frame_preview
    img = extract_image(data)
    if img:
        logger.info(f"→ photo ({len(img)} bytes): {cam}")
        await tg_send_photo(caption, img, chat_id)
        return True

    # Видео
    if DOWNLOAD_VIDEO:
        if not _jwt_token:
            await refresh() or await login()
        video = await download_video(data)
        if video:
            logger.info(f"→ video ({len(video)} bytes): {cam}")
            await tg_send_video(caption, video, chat_id)
            return True

    logger.info(f"→ text: {cam}")
    await tg_send_message(caption, chat_id)
    return True


async def download_video(data: dict) -> bytes | None:
    if not _jwt_token:
        return None
    media = data.get("camera_id", data.get("camera", data.get("media", "")))
    start = data.get("start_at", data.get("opened_at", 0)) or 0
    end = data.get("end_at", data.get("closed_at", 0)) or 0
    if not media or not start:
        return None

    if start > 1e12:
        start = int(start / 1000)
    if end and end > 1e12:
        end = int(end / 1000)
    if not end or end <= start:
        end = start + 10

    headers = jwt_headers()
    urls = [
        f"{FLUSSONIC_URL}/{media}/archive-{start}-{end}.mp4",
        f"{FLUSSONIC_URL}/{media}/preview.mp4",
    ]
    for url in urls:
        try:
            async with _session.get(url, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=60, sock_read=30)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if len(data) > 500:
                        return data
        except Exception:
            pass
    return None


async def handle_motion_from_ha(request: web.Request) -> web.Response:
    """Принять motion-событие от HA и отправить SMTP на Flussonic camera_alarm."""
    import smtplib
    from email.mime.text import MIMEText

    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")

    cam_id = data.get("camera_id", "kalitka-be56b9bd2d")
    stream = data.get("stream", cam_id)
    ts = data.get("timestamp", "")

    logger.info(f"HA motion: {stream} → SMTP {cam_id}")
    try:
        msg = MIMEText(f"Motion on {stream}\n{ts}", "plain", "utf-8")
        msg["From"] = "ha@ivstar.net"
        msg["To"] = f"{cam_id}@video.iks-online.net"
        msg["Subject"] = f"Motion: {stream}"

        s = smtplib.SMTP("video.iks-online.net", 1025, timeout=10)
        s.login("pechkin", "russianpost")
        s.sendmail("ha@ivstar.net", f"{cam_id}@video.iks-online.net", msg.as_string())
        s.quit()
        logger.info(f"SMTP OK: {cam_id}")
        return web.Response(status=200, text="ok")
    except Exception as e:
        logger.error(f"SMTP: {e}")
        return web.Response(status=500, text=str(e))


async def health(_request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def main():
    global _session, _tg_session
    token = os.environ.get("HTTP_TOKEN", "")

    @web.middleware
    async def token_middleware(request, handler):
        if token and request.headers.get("X-Token", "") != token:
            return web.Response(status=403, text="forbidden")
        if token and request.query.get("token", "") != token and request.method == "GET":
            return web.Response(status=403, text="forbidden")
        return await handler(request)

    app = web.Application(middlewares=[token_middleware])
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_post("/motion", handle_motion_from_ha)
    app.router.add_get("/health", health)

    async with (
        aiohttp.ClientSession() as watcher_session,
        aiohttp.ClientSession(proxy=PROXY_URL or None) as tg_s,
    ):
        _session = watcher_session
        _tg_session = tg_s
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, LISTEN_HOST, LISTEN_PORT)
        await site.start()
        logger.info(f"Listening on {LISTEN_HOST}:{LISTEN_PORT}/webhook")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
