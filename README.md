# Flussonic Watcher → Telegram

Мониторинг событий Flussonic Watcher (распознавание лиц, номеров, движение) с отправкой уведомлений в Telegram.

## Возможности

- **Распознавание лиц** — поиск персоны по времени через API, отправка фото/видео в Telegram
- **Распознавание номеров** — цвет, модель, сторона автомобиля, назначение
- **Движение** — уведомления с anti-spam rate-limiting
- **Раздельные Telegram-чаты** — лицо/номера/движение в разные чаты
- **Webhook-режим** — приём событий напрямую от Flussonic
- **Nomerogram** — проверка госномеров через API

## Компоненты

| Файл | Назначение |
|------|-----------|
| `poll_watcher.py` | Опрашивает Watcher API v3 и отправляет события в Telegram |
| `webhook_receiver.py` | HTTP-сервер для приёма webhook-событий от Flussonic |
| `deploy.sh` | Развёртывание systemd-служб на сервере |

## Установка

```bash
# Клонировать репозиторий
git clone https://github.com/dmsovenko-ship-it/flussonic.git /opt/flussonic-watcher-poll

# Установить зависимости
pip install -r requirements.txt

# Скопировать .env.example и заполнить
cp .env.example .env
nano .env

# Запустить установку служб
bash deploy.sh
```

## Переменные окружения (`.env`)

Полный список переменных в `.env.example`. Основные:

```bash
FLUSSONIC_URL=http://172.20.1.21
FLUSSONIC_LOGIN=admin
FLUSSONIC_PASSWORD=your_password
TG_TOKEN=your_bot_token
TG_CHAT_ID=-1001111111111
TG_CHAT_ID_FACE=-1002222222222
TG_CHAT_ID_PLATE=-1003333333333
TG_CHAT_ID_MOTION=-1004444444444
```

## Управление службами

```bash
systemctl start|stop|restart watcher-poll watcher-webhook
systemctl status watcher-poll watcher-webhook
journalctl -u watcher-poll -f
journalctl -u watcher-webhook -f
```

## GitHub Agent

Работает через `/oc` в issue/PR. Требуется secret `DEEPSEEK_API_KEY` в настройках репозитория.
