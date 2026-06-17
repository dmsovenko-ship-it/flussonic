#!/bin/bash
# Развёртывание systemd-служб для Flussonic Watcher → Telegram

set -e

INSTALL_DIR="/opt/flussonic-watcher-poll"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Установка Flussonic Watcher служб ==="
echo "Директория: $INSTALL_DIR"

# Копирование файлов
if [ "$SCRIPT_DIR" != "$INSTALL_DIR" ]; then
    echo "Копирование файлов в $INSTALL_DIR..."
    sudo mkdir -p "$INSTALL_DIR"
    sudo cp "$SCRIPT_DIR"/*.py "$INSTALL_DIR/"
    sudo cp "$SCRIPT_DIR"/requirements.txt "$INSTALL_DIR/"
    if [ -f "$SCRIPT_DIR/.env" ]; then
        sudo cp "$SCRIPT_DIR/.env" "$INSTALL_DIR/"
        sudo chmod 600 "$INSTALL_DIR/.env"
    fi
fi

# Установка зависимостей
echo "Установка Python-зависимостей..."
sudo pip install -r "$INSTALL_DIR/requirements.txt"

# --- watcher-poll.service ---
echo "Создание watcher-poll.service..."
sudo tee /etc/systemd/system/watcher-poll.service > /dev/null <<EOF
[Unit]
Description=Flussonic Watcher poll → Telegram
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/bin/python3 $INSTALL_DIR/poll_watcher.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# --- watcher-webhook.service ---
echo "Создание watcher-webhook.service..."
sudo tee /etc/systemd/system/watcher-webhook.service > /dev/null <<EOF
[Unit]
Description=Flussonic webhook → Telegram
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/bin/python3 $INSTALL_DIR/webhook_receiver.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Применение
sudo systemctl daemon-reload
sudo systemctl enable watcher-poll watcher-webhook
sudo systemctl restart watcher-poll watcher-webhook

echo ""
echo "=== Готово ==="
echo "Статус служб:"
sudo systemctl status watcher-poll watcher-webhook --no-pager -l
echo ""
echo "Логи:"
echo "  journalctl -u watcher-poll -f"
echo "  journalctl -u watcher-webhook -f"
