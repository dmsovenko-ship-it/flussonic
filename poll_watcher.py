from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import smtplib
import time
import traceback
from datetime import datetime

import aiohttp

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
logger = logging.getLogger("watcher-poll")

# --- Конфигурация ----------------------------------------------------------
FLUSSONIC_URL  = os.environ.get("FLUSSONIC_URL", "http://172.20.1.21").rstrip("/")
FLUSSONIC_USER = os.environ.get("FLUSSONIC_LOGIN", "")
FLUSSONIC_PASS = os.environ.get("FLUSSONIC_PASSWORD", "")

TG_TOKEN   = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
TG_API_URL = os.environ.get("TG_API_URL", "https://api.telegram.org").rstrip("/")

TG_CHAT_ID_FACE   = os.environ.get("TG_CHAT_ID_FACE", TG_CHAT_ID)
TG_CHAT_ID_PLATE  = os.environ.get("TG_CHAT_ID_PLATE", TG_CHAT_ID)
TG_CHAT_ID_MOTION = os.environ.get("TG_CHAT_ID_MOTION", TG_CHAT_ID)

NOMEROGRAM_KEY = os.environ.get("NOMEROGRAM_KEY", "")

PROXY_URL = os.environ.get("PROXY_URL", "").strip()  # http://proxy:port или socks5://proxy:port

# SMTP для email-уведомлений
SMTP_ENABLED = os.environ.get("SMTP_ENABLED", "false").lower() in ("true", "1", "yes")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.example.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM = os.environ.get("SMTP_FROM", "")
SMTP_TO = os.environ.get("SMTP_TO", "")
SMTP_SUBJECT = os.environ.get("SMTP_SUBJECT", "Flussonic: движение {{ stream }}")

DOWNLOAD_MEDIA = os.environ.get("DOWNLOAD_MEDIA", "true").lower() not in ("false", "0", "no")
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "5"))
RATE_LIMIT_SEC = int(os.environ.get("RATE_LIMIT_SEC", "10"))

# Анти-спам: макс событий за интервал
RATE_LIMIT_COUNT = int(os.environ.get("RATE_LIMIT_COUNT", "3"))    # макс событий
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "300"))  # за N секунд
MAX_EVENT_AGE_HOURS = int(os.environ.get("MAX_EVENT_AGE_HOURS", "24"))

CAMERA_FILTER = os.environ.get("CAMERA_FILTER", "").split(",")  # пусто = все
EVENT_TYPES   = os.environ.get("EVENT_TYPES", "face,plate,motion").split(",")

# --- Состояние ------------------------------------------------------------
_last_seen: dict[str, float] = {}       # event_id -> timestamp отправки
_sent_events: set[str] = set()          # уже отправленные id
_known_names: dict[str, str] = {}       # face_id -> person_name (кэш)
_rate_limit_times: list[float] = []      # таймстемпы отправленных motion (анти-спам)


def _rate_limit_ok() -> bool:
    """Проверка: не превышен ли лимит RATE_LIMIT_COUNT событий за RATE_LIMIT_WINDOW."""
    if RATE_LIMIT_COUNT <= 0 or RATE_LIMIT_WINDOW <= 0:
        return True
    now = time.monotonic()
    # Оставляем только события в окне
    cutoff = now - RATE_LIMIT_WINDOW
    while _rate_limit_times and _rate_limit_times[0] < cutoff:
        _rate_limit_times.pop(0)
    if len(_rate_limit_times) >= RATE_LIMIT_COUNT:
        return False
    _rate_limit_times.append(now)
    return True

# --- API Watcher -----------------------------------------------------------

_basic_auth_header: dict[str, str] = {}
if FLUSSONIC_PASS:
    _basic_auth_header = {"Authorization": aiohttp.BasicAuth(FLUSSONIC_USER, FLUSSONIC_PASS).encode()}
_jwt_token: str = ""
_refresh_token: str = ""
_jwt_lock = asyncio.Lock()


async def _login(session: aiohttp.ClientSession) -> str:
    """Получить JWT-токен (Basic Auth → Client API v3)."""
    global _jwt_token, _refresh_token
    async with _jwt_lock:
        if _jwt_token:
            return _jwt_token
        url = f"{FLUSSONIC_URL}/watcher/client-api/v3/login"
        try:
            async with session.post(url, headers=_basic_auth_header,
                                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _jwt_token = data.get("access_token", "")
                    _refresh_token = data.get("refresh_token", "")
                    logger.info("JWT login OK (Basic Auth)")
                    return _jwt_token
                logger.error(f"Login failed {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"Login error: {e}")
        return ""


async def _refresh(session: aiohttp.ClientSession) -> str:
    """Обновить JWT через refresh_token (без пароля)."""
    global _jwt_token, _refresh_token
    async with _jwt_lock:
        if not _refresh_token:
            return ""
        url = f"{FLUSSONIC_URL}/watcher/client-api/v3/login"
        headers = {"Authorization": f"Bearer {_refresh_token}"}
        try:
            async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _jwt_token = data.get("access_token", "")
                    new_refresh = data.get("refresh_token", "")
                    if new_refresh:
                        _refresh_token = new_refresh
                    logger.info("JWT refreshed OK")
                    return _jwt_token
                logger.warning(f"Refresh failed {resp.status}: {(await resp.text())[:200]}")
                _refresh_token = ""
        except Exception as e:
            logger.warning(f"Refresh error: {e}")
        return ""


def _bearer() -> dict:
    """Заголовок с Bearer токеном."""
    return {"Authorization": f"Bearer {_jwt_token}"} if _jwt_token else {}


async def _api_get(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> dict | list | None:
    """GET-запрос к Watcher API v3 с Bearer-токеном."""
    headers = _bearer()
    url = f"{FLUSSONIC_URL}{path}"
    if not params:
        params = {}
    try:
        async with session.get(
            url, params=params, headers=headers, allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            if resp.status in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location", "")
                if loc:
                    if loc.startswith("https://"):
                        loc = "http://" + loc[8:]
                    logger.debug(f"API redirect {resp.status} → {loc[:100]}")
                    async with session.get(loc, headers=headers,
                                           timeout=aiohttp.ClientTimeout(total=10)) as r2:
                        if r2.status == 200:
                            return await r2.json()
                        logger.warning(f"Redirect target {r2.status}: {loc[:100]}")
                    return None
            if resp.status == 401:
                logger.warning("JWT expired, refreshing...")
                global _jwt_token
                _jwt_token = ""
                return None
            if resp.status == 403:
                logger.warning(f"API 403 Forbidden on {url}")
                return None
            text = await resp.text()
            logger.warning(f"API {resp.status} on {url} params={params}: {text[:300]}")
            return None
    except Exception as e:
        logger.error(f"API error on {url}: {e}")
        return None


async def fetch_events(session: aiohttp.ClientSession, since: int = 0, _retry: int = 0) -> list[dict]:
    if _retry > 2:
        logger.warning("fetch_events: превышен лимит попыток")
        return []
    if not _jwt_token:
        logger.debug("fetch_events: нет JWT, логинимся...")
        if not await _login(session):
            logger.warning("fetch_events: логин не удался")
            return []

    path = "/watcher/client-api/v3/episodes"
    all_events = []

    # Типы эпизодов для запроса (фактические значения API Watcher v3)
    type_map = {"face": "face", "plate": "vehicle", "motion": "generic"}
    wanted_types = [type_map[t] for t in EVENT_TYPES if t in type_map]
    if not wanted_types:
        wanted_types = ["generic", "face", "vehicle"]

    # Используем updated_at_gt для получения только новых эпизодов
    FETCH_LIMIT = 2000
    for etype in wanted_types:
        params: dict[str, object] = {"limit": FETCH_LIMIT, "episode_type": etype}
        if since > 0:
            params["updated_at_gt"] = since
        logger.debug(f"fetch_events: запрос etype={etype}, limit={FETCH_LIMIT}")
        result = await _api_get(session, path, params)
        if result is None:
            if _jwt_token == "":
                logger.debug(f"fetch_events: JWT сброшен, пробую refresh (попытка {_retry + 1})")
                if not await _refresh(session):
                    await _login(session)
                return await fetch_events(session, since, _retry + 1)
            logger.debug(f"fetch_events: etype={etype} вернул None (ошибка API)")
            continue
        count = 0
        if isinstance(result, list):
            count = len(result)
            all_events.extend(result)
        elif isinstance(result, dict):
            for key in ("episodes", "events", "items", "data", "results", "rows"):
                if key in result and isinstance(result[key], list):
                    count = len(result[key])
                    all_events.extend(result[key])
                    break
        logger.debug(f"fetch_events: etype={etype} → {count} эпизодов")

    logger.debug(f"fetch_events: всего {len(all_events)} эпизодов")

    all_events.sort(key=lambda e: e.get("opened_at", 0), reverse=True)
    return all_events


async def _download_video(session: aiohttp.ClientSession, event: dict) -> bytes | None:
    """Скачать видео эпизода из архива по playback_token."""
    media = event.get("media", "")
    token = event.get("playback_token", "")
    endpoint = event.get("streaming_endpoint", "")
    opened = event.get("opened_at", 0)
    closed = event.get("closed_at", event.get("updated_at", opened + 10000))

    if not media or not token:
        logger.debug("download_video: нет media или token")
        return None

    t_from = int(opened / 1000) if opened > 1e12 else opened
    t_to = int(closed / 1000) if closed > 1e12 else closed
    if t_to <= t_from:
        t_to = t_from + 10

    urls = []
    if endpoint:
        urls.append(f"{endpoint.rstrip('/')}/{media}/archive-{t_from}-{t_to}.mp4?token={token}")
    urls.append(f"{FLUSSONIC_URL}/{media}/archive-{t_from}-{t_to}.mp4?token={token}")

    logger.debug(f"download_video: пробуем {len(urls)} URL")
    for url in urls:
        try:
            logger.debug(f"download_video: GET {url[:120]}...")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60, sock_read=30)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    logger.debug(f"download_video: OK {len(data)} bytes")
                    return data
                logger.debug(f"download_video: HTTP {resp.status}")
        except Exception as e:
            logger.debug(f"download_video: ошибка {type(e).__name__}: {e}")
    logger.debug("download_video: все URL не сработали")
    return None


def send_email(stream: str, body: str):
    """Отправить email через SMTP (блокирующий вызов в потоке)."""
    import threading
    def _send():
        try:
            subject = SMTP_SUBJECT.replace("{{ stream }}", stream)
            msg = f"From: {SMTP_FROM}\r\nTo: {SMTP_TO}\r\nSubject: {subject}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{body}"
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
                s.starttls()
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(SMTP_FROM, SMTP_TO, msg.encode('utf-8'))
            logger.info(f"Email sent: {subject}")
        except Exception as e:
            logger.error(f"SMTP error: {e}")
    threading.Thread(target=_send, daemon=True).start()


# --- Telegram --------------------------------------------------------------

async def tg_send_video(session: aiohttp.ClientSession, caption: str, video: bytes, chat_id: str = "") -> None:
    api = f"{TG_API_URL}/bot{TG_TOKEN}"
    form = aiohttp.FormData()
    form.add_field("chat_id", chat_id or TG_CHAT_ID)
    form.add_field("caption", caption)
    form.add_field("parse_mode", "HTML")
    form.add_field("video", video, filename="event.mp4", content_type="video/mp4")
    try:
        async with session.post(f"{api}/sendVideo", data=form) as resp:
            if resp.status != 200:
                logger.error(f"TG sendVideo failed: {resp.status} {(await resp.text())[:200]}")
            else:
                logger.debug(f"TG sendVideo OK ({len(video)} bytes)")
    except Exception as e:
        logger.error(f"TG sendVideo error: {type(e).__name__}: {e}")


async def tg_send_photo(session: aiohttp.ClientSession, caption: str, img: bytes, chat_id: str = "") -> None:
    api = f"{TG_API_URL}/bot{TG_TOKEN}"
    form = aiohttp.FormData()
    form.add_field("chat_id", chat_id or TG_CHAT_ID)
    form.add_field("caption", caption)
    form.add_field("parse_mode", "HTML")
    form.add_field("photo", img, filename="snapshot.jpg", content_type="image/jpeg")
    try:
        async with session.post(f"{api}/sendPhoto", data=form) as resp:
            if resp.status != 200:
                logger.error(f"TG sendPhoto failed: {resp.status} {(await resp.text())[:200]}")
            else:
                logger.debug(f"TG sendPhoto OK ({len(img)} bytes)")
    except Exception as e:
        logger.error(f"TG sendPhoto error: {type(e).__name__}: {e}")


async def tg_send_message(session: aiohttp.ClientSession, text: str, chat_id: str = "") -> None:
    api = f"{TG_API_URL}/bot{TG_TOKEN}"
    payload = {"chat_id": chat_id or TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(f"{api}/sendMessage", json=payload) as resp:
            if resp.status != 200:
                logger.error(f"TG sendMessage failed: {resp.status} {(await resp.text())[:200]}")
            else:
                logger.debug(f"TG sendMessage OK ({len(text)} chars)")
    except Exception as e:
        logger.error(f"TG sendMessage error: {type(e).__name__}: {e}")


# --- Словари перевода ----------------------------------------------------

VEHICLE_PURPOSE_RU: dict[str, str] = {
    "regular": "обычный",
    "emergency": "экстренный",
    "special": "специальный",
    "public": "общественный",
    "cargo": "грузовой",
    "taxi": "такси",
    "car_sharing": "каршеринг",
}

VEHICLE_EMERGENCY_RU: dict[str, str] = {
    "ambulance": "скорая помощь",
    "police": "полиция",
    "fire": "пожарная",
    "mchs": "МЧС",
    "emergency": "аварийная",
    "military": "военный",
}

FACING_RU: dict[str, str] = {
    "front": "перед",
    "rear": "зад",
    "left": "лево",
    "right": "право",
}

VEHICLE_COLOR_RU: dict[str, str] = {
    "black": "чёрный",
    "white": "белый",
    "gray": "серый",
    "grey": "серый",
    "silver": "серебристый",
    "red": "красный",
    "blue": "синий",
    "green": "зелёный",
    "yellow": "жёлтый",
    "cyan": "голубой",
    "magenta": "пурпурный",
    "orange": "оранжевый",
    "brown": "коричневый",
    "purple": "фиолетовый",
    "pink": "розовый",
    "beige": "бежевый",
    "gold": "золотой",
    "maroon": "бордовый",
    "navy": "тёмно-синий",
    "teal": "бирюзовый",
    "violet": "лиловый",
    "turquoise": "бирюзовый",
    "indigo": "индиго",
    "lime": "лаймовый",
    "olive": "оливковый",
    "tan": "бежевый",
    "coral": "коралловый",
}

# --- Обработка событий -----------------------------------------------------

def format_ts(raw) -> str:
    if not raw:
        return ""
    try:
        if isinstance(raw, (int, float)):
            # Миллисекунды -> секунды
            ts = raw / 1000 if raw > 1e12 else raw
            return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return str(raw)[:19]


def _field(event: dict, *keys: str):
    """Достать поле из события, пробуя несколько ключей (включая вложенные объекты)."""
    details = event.get("details", {})
    if not isinstance(details, dict):
        details = {}

    for k in keys:
        # Прямой ключ
        if k in event:
            val = event[k]
            if isinstance(val, dict) and "name" in val:
                return val["name"]
            return val
        # Вложенный объект stream / cam
        for parent in ("stream", "cam", "camera"):
            obj = event.get(parent)
            if isinstance(obj, dict) and k in obj:
                return obj[k]
        # В details
        if k in details:
            val = details[k]
            if isinstance(val, dict) and "name" in val:
                return val["name"]
            return val

    # Для ключа 'camera_id'/'camera'/'stream' — извлекаем имя из stream/cam
    for k in keys:
        if k in ("camera_id", "camera", "stream", "cam"):
            for parent in ("stream", "cam", "camera"):
                obj = event.get(parent)
                if isinstance(obj, dict):
                    return obj.get("name", obj.get("title", ""))
        # Пробуем media (часто равно stream.name)
        media = event.get("media", "")
        if media and k in ("camera_id", "camera", "stream", "cam"):
            return media

    return ""


def _event_type(event: dict) -> str:
    """Определить тип события по episode_type (фактические значения API v3)."""
    ept = str(event.get("episode_type", "")).lower()
    if ept == "face":
        return "face"
    if ept == "vehicle":
        return "plate"
    if ept == "generic":
        return "motion"

    # Fallback по содержимому
    if event.get("matched_persons") or event.get("detections"):
        return "face"
    if event.get("license_plate_text") or event.get("vehicle_model"):
        return "plate"

    return "motion"


def _cam_name(event: dict) -> str:
    """Человеческое имя камеры (title если есть, иначе name)."""
    for parent in ("stream", "cam", "camera"):
        obj = event.get(parent)
        if isinstance(obj, dict):
            return obj.get("title", obj.get("name", ""))
    # Fallback: media
    return event.get("media", "")


def should_skip(event: dict) -> bool:
    eid = str(event.get("episode_id", _field(event, "id", "event_id")))
    if eid and eid in _sent_events:
        logger.debug(f"Пропуск: дубликат episode_id={eid}")
        return True

    if RATE_LIMIT_SEC:
        now = time.monotonic()
        cam = _cam_name(event)
        ev_type = _event_type(event)
        if ev_type == "plate":
            plate = str(event.get("license_plate_text", _field(event, "number", "plate", "plate_number")))
            key = f"{cam}:{plate}"
        else:
            key = f"{cam}:{ev_type}"
        if now - _last_seen.get(key, 0) < RATE_LIMIT_SEC:
            logger.debug(f"Пропуск: rate-limit {key}")
            return True
        _last_seen[key] = now

    if eid:
        _sent_events.add(eid)
    return False


def camera_allowed(event: dict) -> bool:
    if not CAMERA_FILTER or CAMERA_FILTER == [""]:
        return True
    cam_name = _cam_name(event)
    cam_raw = _field(event, "stream", "cam", "camera_id", "camera", "media")
    return cam_name in CAMERA_FILTER or cam_raw in CAMERA_FILTER or "all" in CAMERA_FILTER


async def _fetch_preview_image(session: aiohttp.ClientSession, event: dict) -> bytes | None:
    """Достать изображение из эпизода: thumbnail (base64) → preview/frame_preview (URL либо base64)."""
    # 1. thumbnail из detections (всегда base64)
    detections = event.get("detections", [])
    if isinstance(detections, list) and detections:
        d = detections[0]
        if isinstance(d, dict):
            thumb = d.get("thumbnail", {})
            if isinstance(thumb, dict):
                b64 = thumb.get("data", "")
                if b64:
                    try:
                        img = base64.b64decode(b64)
                        logger.debug(f"Изображение из thumbnail: {len(img)} bytes")
                        return img
                    except Exception as e:
                        logger.debug(f"thumbnail decode failed: {e}")

    # 2. preview / frame_preview — пробуем как URL, затем как base64
    for fld in ("frame_preview", "preview"):
        val = event.get(fld, "")
        if not val or len(val) <= 100:
            continue

        # Пробуем как URL (абсолютный или относительный)
        url = val if val.startswith("http") else f"{FLUSSONIC_URL}/{val.lstrip('/')}"
        try:
            headers = _bearer()
            async with session.get(url, headers=headers, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    img = await resp.read()
                    if len(img) > 500:
                        logger.debug(f"Изображение из URL {fld}: {len(img)} bytes")
                        return img
                    logger.debug(f"URL {fld} вернул {len(img)} bytes — маловато")
                else:
                    logger.debug(f"URL {fld} вернул {resp.status}")
        except Exception as e:
            logger.debug(f"URL {fld} ошибка: {type(e).__name__}: {e}")

        # Пробуем как base64
        try:
            img = base64.b64decode(val)
            if len(img) > 100:
                logger.debug(f"Изображение из {fld} (base64): {len(img)} bytes")
                return img
        except Exception:
            pass

    logger.debug("Изображение не найдено")
    return None


async def _lookup_person_by_time(session: aiohttp.ClientSession, episode: dict) -> str | None:
    ep_start = episode.get("started_at", episode.get("opened_at", 0))
    if not ep_start:
        logger.debug("Person lookup: нет started_at/opened_at у эпизода")
        return None
    result = await _api_get(session, "/watcher/client-api/v3/persons", {"limit": 200})
    if not result or not isinstance(result, dict):
        logger.debug("Person lookup: API persons вернул пустой/невалидный ответ")
        return None
    persons = result.get("persons", [])
    logger.debug(f"Person lookup: получено {len(persons)} персон, ep_start={ep_start}")
    window = 20000  # ±20 сек в мс
    best_name = ""
    best_distance = window
    for p in persons:
        first_seen = p.get("first_seen_at", 0)
        last_seen = p.get("last_seen_at", 0)
        name = p.get("name", "")
        if not name or name == "unknown":
            continue
        dist = None
        if last_seen and abs(last_seen - ep_start) < best_distance:
            dist = abs(last_seen - ep_start)
        if first_seen and abs(first_seen - ep_start) < best_distance:
            d = abs(first_seen - ep_start)
            if dist is None or d < dist:
                dist = d
        if dist is not None:
            logger.debug(f"  кандидат: {name} first_seen={first_seen} last_seen={last_seen} dist={dist}ms")
            best_distance = dist
            best_name = name
    if best_name:
        logger.info(f"Person match: {best_name} (distance={best_distance}ms, ep_start={ep_start})")
        return best_name
    logger.info(f"Person lookup: совпадений не найдено для ep_start={ep_start} (проверено {len(persons)} персон)")
    return None


async def handle_face(watcher: aiohttp.ClientSession, tg: aiohttp.ClientSession, event: dict) -> None:
    eid = event.get("episode_id", "?")
    logger.info(f"Обработка face episode_id={eid}")
    if should_skip(event):
        return

    stream = _cam_name(event)

    # matched_persons[0].person.name
    persons = event.get("matched_persons", [])
    person = ""
    person_id = ""
    match_score = 0
    if persons and isinstance(persons, list):
        p = persons[0]
        if isinstance(p, dict):
            person_obj = p.get("person", {})
            person = person_obj.get("name", "")
            person_id = str(person_obj.get("person_id", ""))
            match_score = p.get("match_score", 0)

    # confidence из detections
    detections = event.get("detections", [])
    confidence = 0
    if detections and isinstance(detections, list):
        d = detections[0]
        if isinstance(d, dict):
            confidence = d.get("confidence", 0)

    ts = format_ts(event.get("opened_at", 0))

    name = "Неизвестный"
    # Пробуем найти персону по времени (last_seen_at ≈ started_at)
    try:
        pname = await _lookup_person_by_time(watcher, event)
        if pname:
            name = pname
    except Exception:
        pass
    if name == "Неизвестный" and person:
        name = person
    logger.debug(f"Face: {name} (id={person_id}, match={match_score}, conf={confidence})")
    lines = [f"👤 {name} — {stream}"]
    if person_id:
        lines.append(f"  ID: {person_id}")
    if match_score:
        pct = int(float(match_score) * 100) if float(match_score) < 10 else int(float(match_score))
        lines.append(f"  Совпадение: {pct}%")
    if confidence:
        pct = int(float(confidence) * 100) if float(confidence) < 10 else int(float(confidence))
        lines.append(f"  Точность: {pct}%")
    if ts:
        lines.append(f"  {ts}")

    caption = "\n".join(lines)

    if DOWNLOAD_MEDIA:
        img = await _fetch_preview_image(watcher, event)
        if img:
            logger.info(f"Face → photo ({len(img)} bytes): {stream}")
            await tg_send_photo(tg, caption, img, TG_CHAT_ID_FACE)
        else:
            vid = await _download_video(watcher, event)
            if vid:
                logger.info(f"Face → video ({len(vid)} bytes): {stream}")
                await tg_send_video(tg, caption, vid, TG_CHAT_ID_FACE)
            else:
                logger.info(f"Face → text only: {stream}")
                await tg_send_message(tg, caption, TG_CHAT_ID_FACE)
    else:
        logger.info(f"Face → text (media off): {stream}")
        await tg_send_message(tg, caption, TG_CHAT_ID_FACE)


async def handle_plate(watcher: aiohttp.ClientSession, tg: aiohttp.ClientSession, event: dict) -> None:
    eid = event.get("episode_id", "?")
    logger.info(f"Обработка vehicle episode_id={eid}")
    if should_skip(event):
        return

    stream = _cam_name(event)
    plate = str(event.get("license_plate_text", "")).upper().strip()
    action = str(event.get("vehicle_purpose", event.get("object_action", event.get("activity_type", ""))))
    ts = format_ts(event.get("opened_at", 0))

    # Модель и цвет авто (из vehicle эпизода)
    models = event.get("vehicle_model", [])
    model_name = models[0].get("name", "") if models else ""
    model_conf = models[0].get("confidence", 0) if models else 0
    colors = event.get("vehicle_color", [])
    color_name = colors[0].get("color", "") if colors else ""
    color_conf = colors[0].get("confidence", 0) if colors else 0
    facing = event.get("vehicle_facing_side", "")
    emergency = event.get("vehicle_emergency_subtype", "")
    missing = event.get("license_plate_missing", False)

    logger.debug(f"Plate: {plate or 'нет'}, model={model_name}, color={color_name}, facing={facing}, missing={missing}")

    # Собираем caption
    lines = []
    if plate and not missing:
        lines.append(f"🚗 Госномер: <b>{plate}</b> — {stream}")
    elif missing:
        lines.append(f"🚗 Без номера — {stream}")
    else:
        lines.append(f"🚗 Автомобиль — {stream}")
    if action:
        action_ru = VEHICLE_PURPOSE_RU.get(action, action)
        lines.append(f"  Назначение: {action_ru}")
    if emergency:
        emergency_ru = VEHICLE_EMERGENCY_RU.get(emergency, emergency)
        lines.append(f"  Спецтранспорт: {emergency_ru}")
    if model_name:
        pct = int(float(model_conf) * 100) if float(model_conf) < 10 else int(float(model_conf))
        lines.append(f"  Модель: {model_name} ({pct}%)")
    if color_name:
        pct = int(float(color_conf) * 100) if float(color_conf) < 10 else int(float(color_conf))
        color_ru = VEHICLE_COLOR_RU.get(color_name, color_name)
        lines.append(f"  Цвет: {color_ru} ({pct}%)")
    if facing:
        side = FACING_RU.get(facing, facing)
        lines.append(f"  Сторона: {side}")
    if ts:
        lines.append(f"  {ts}")

    # Nomerogram lookup
    if plate and NOMEROGRAM_KEY:
        org = await nomerogram_lookup(tg, plate)
        if org:
            lines.append(f"  Nomerogram: {org}")
    caption = "\n".join(lines)

    if DOWNLOAD_MEDIA:
        img = await _fetch_preview_image(watcher, event)
        if img:
            logger.info(f"Plate → photo ({len(img)} bytes): {stream}")
            await tg_send_photo(tg, caption, img, TG_CHAT_ID_PLATE)
        else:
            vid = await _download_video(watcher, event)
            if vid:
                logger.info(f"Plate → video ({len(vid)} bytes): {stream}")
                await tg_send_video(tg, caption, vid, TG_CHAT_ID_PLATE)
            else:
                logger.info(f"Plate → text only: {stream}")
                await tg_send_message(tg, caption, TG_CHAT_ID_PLATE)
    else:
        logger.info(f"Plate → text (media off): {stream}")
        await tg_send_message(tg, caption, TG_CHAT_ID_PLATE)


async def handle_motion(watcher: aiohttp.ClientSession, tg: aiohttp.ClientSession, event: dict) -> None:
    eid = event.get("episode_id", "?")
    logger.info(f"Обработка motion episode_id={eid}")
    if should_skip(event):
        return

    stream = _cam_name(event)
    ts = format_ts(event.get("opened_at", 0))
    title = event.get("title", "")
    desc = event.get("description", "")

    logger.debug(f"Motion: {stream}, title={title}")

    lines = [f"🎬 Движение: {stream}"]
    if title:
        lines.append(f"  {title}")
    if desc and desc != title:
        lines.append(f"  {desc}")
    if ts:
        lines.append(f"  {ts}")

    caption = "\n".join(lines)

    if SMTP_ENABLED:
        send_email(stream, caption)

    if DOWNLOAD_MEDIA:
        img = await _fetch_preview_image(watcher, event)
        if img:
            if not _rate_limit_ok():
                logger.info(f"Motion → SKIP (rate limit): {stream}")
                return
            logger.info(f"Motion → photo ({len(img)} bytes): {stream}")
            await tg_send_photo(tg, caption, img, TG_CHAT_ID_MOTION)
        else:
            vid = await _download_video(watcher, event)
            if vid:
                if not _rate_limit_ok():
                    logger.info(f"Motion → SKIP (rate limit): {stream}")
                    return
                logger.info(f"Motion → video ({len(vid)} bytes): {stream}")
                await tg_send_video(tg, caption, vid, TG_CHAT_ID_MOTION)
            else:
                logger.debug(f"Motion → skip (no media): {stream}")
                # Не шлём текст без картинки — спам
    else:
        logger.debug(f"Motion → skip (DOWNLOAD_MEDIA=false): {stream}")


async def nomerogram_lookup(session: aiohttp.ClientSession, plate: str) -> str | None:
    if not NOMEROGRAM_KEY:
        return None
    logger.debug(f"Nomerogram: запрос для {plate}")
    try:
        async with session.get(
            "https://nomerogram.ru/api/v1/check",
            params={"plate": plate, "key": NOMEROGRAM_KEY},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get("organization", data.get("owner", ""))
                if result:
                    logger.debug(f"Nomerogram: {plate} → {result}")
                return result
    except Exception as e:
        logger.debug(f"Nomerogram error: {type(e).__name__}: {e}")
    return None


# --- Главный цикл ---------------------------------------------------------

async def main():
    if not TG_TOKEN or not TG_CHAT_ID:
        logger.critical("TG_TOKEN и TG_CHAT_ID обязательны")
        return

    logger.info(f"Опрос Watcher каждые {POLL_INTERVAL}с, события: {EVENT_TYPES}, камеры: {CAMERA_FILTER or 'все'}")
    last_ts = int(time.time() * 1000) - 120000

    # URL webhook_receiver.py для пересылки face-эпизодов
    WEBHOOK_FWD_URL = os.environ.get("WEBHOOK_FWD_URL", "").strip()  # пропускаем старые события

    handlers = {
        "face": handle_face,
        "plate": handle_plate,
        "motion": handle_motion,
    }
    _first_log = True

    async with (
        aiohttp.ClientSession() as watcher_session,
        aiohttp.ClientSession(proxy=PROXY_URL or None) as tg_session,
    ):
        while True:
            try:
                logger.debug("--- Цикл опроса ---")
                events = await fetch_events(watcher_session, since=last_ts)

                if events:
                    # Стартовый цикл: запоминаем id, но не обрабатываем
                    if _first_log:
                        seeded = 0
                        for e in events:
                            eid = str(e.get("episode_id", ""))
                            if eid:
                                _sent_events.add(eid)
                                seeded += 1
                        logger.info(f"Старт: запомнено {seeded} старых episode_id (пропущены), жду новых событий...")
                        cameras = sorted(set(_cam_name(e) for e in events))
                        logger.info(f"Камеры в API: {cameras}")
                        types = sorted(set(_event_type(e) for e in events))
                        logger.info(f"Типы событий: {types}")
                        for e in events:
                            if e.get("episode_type") != "generic":
                                logger.info(f"Пример {e.get('episode_type')}: {json.dumps(e, indent=2, default=str, ensure_ascii=False)[:1500]}")
                                break
                        _first_log = False
                        await asyncio.sleep(POLL_INTERVAL)
                        continue

                    # Обычный цикл: только новые (неизвестные) episode_id
                    new_events = [e for e in events if str(e.get("episode_id", "")) not in _sent_events]
                    skipped = len(events) - len(new_events)
                    if skipped:
                        logger.debug(f"Пропущено {skipped} уже известных episode_id")
                    events = new_events

                    # Предварительный фильтр: возраст, тип события и камера
                    # Отфильтрованные id тоже добавляем в _sent_events — чтобы не мелькали каждый цикл
                    now_ms = int(time.time() * 1000)
                    max_age_ms = MAX_EVENT_AGE_HOURS * 3600000
                    eligible = []
                    aged_out = 0
                    for event in events:
                        eid = str(event.get("episode_id", ""))
                        ev_type = _event_type(event)
                        opened = event.get("opened_at", 0)
                        # Пропускаем события старше MAX_EVENT_AGE_HOURS (или без opened_at)
                        if not opened or (now_ms - opened) > max_age_ms:
                            if eid:
                                _sent_events.add(eid)
                            aged_out += 1
                            continue
                        if ev_type not in EVENT_TYPES:
                            if eid:
                                _sent_events.add(eid)
                            continue
                        if not camera_allowed(event):
                            if eid:
                                _sent_events.add(eid)
                            continue
                        eligible.append(event)

                    if aged_out:
                        logger.info(f"Возрастной фильтр: пропущено {aged_out} событий старше {MAX_EVENT_AGE_HOURS}ч")

                    if eligible:
                        cam_stats: dict[str, int] = {}
                        for e in eligible:
                            c = _cam_name(e)
                            cam_stats[c] = cam_stats.get(c, 0) + 1
                        logger.info(f"Опрос: получено {len(eligible)} новых эпизодов, камеры: {cam_stats}")

                    for event in reversed(eligible):
                        ev_type = _event_type(event)
                        eid = event.get("episode_id", "?")
                        cam = _cam_name(event)
                        if ev_type not in EVENT_TYPES:
                            logger.info(f"Пропуск: тип {ev_type} не в EVENT_TYPES={EVENT_TYPES}, камера={cam}, episode_id={eid}")
                            continue
                        if not camera_allowed(event):
                            logger.info(f"Пропуск: камера {cam} не в фильтре {CAMERA_FILTER}, episode_id={eid}")
                            continue

                        handler = handlers.get(ev_type)
                        if handler:
                            # Для face: если включён WEBHOOK_FWD_URL — только форвардим,
                            # иначе обрабатываем локально (избегаем дублей)
                            if ev_type == "face" and WEBHOOK_FWD_URL:
                                resolved_name = ""
                                try:
                                    resolved_name = await _lookup_person_by_time(watcher_session, event) or ""
                                except Exception:
                                    pass
                                try:
                                    fwd = {
                                        "event_type": "FR_RECOGNITION",
                                        "camera_id": _cam_name(event),
                                        "object_id": str(event.get("episode_id", "")),
                                        "start_at": event.get("opened_at", 0),
                                        "stream": event.get("stream", {}),
                                        "preview": event.get("preview", ""),
                                        "frame_preview": event.get("frame_preview", ""),
                                        "detections": event.get("detections", []),
                                        "matched_persons": event.get("matched_persons", []),
                                        "resolved_name": resolved_name,
                                    }
                                    async with watcher_session.post(
                                        f"{WEBHOOK_FWD_URL}/webhook", json=fwd,
                                        timeout=aiohttp.ClientTimeout(total=5),
                                    ) as _:
                                        pass
                                except Exception:
                                    pass
                            else:
                                logger.info(f"Вызов {ev_type} камера={cam} episode_id={eid}")
                                await handler(watcher_session, tg_session, event)
                        else:
                            logger.warning(f"Нет handler для типа {ev_type}")

                    # Обновляем last_ts
                    if events:
                        last_ts = max(e.get("opened_at", 0) for e in events)

            except Exception as e:
                logger.error(f"Poll error: {e}\n{traceback.format_exc()}")

            logger.debug(f"Сон {POLL_INTERVAL}с...")
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
