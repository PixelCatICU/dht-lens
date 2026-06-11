#!/usr/bin/env bash

set -euo pipefail

APP_NAME="dht-lens"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_BIN="$(command -v node)"
NPM_BIN="$(command -v npm)"
RUN_USER="${DHT_LENS_USER:-$(id -un)}"

if [[ "$(id -u)" -eq 0 ]]; then
    echo "Run this script as the application user, not root."
    exit 1
fi

if [[ -z "$NODE_BIN" || -z "$NPM_BIN" ]]; then
    echo "Node.js and npm are required. Run scripts/install-node26-ubuntu.sh first."
    exit 1
fi

npm ci --omit=dev

sudo tee "/etc/systemd/system/${APP_NAME}.service" > /dev/null <<SERVICE
[Unit]
Description=DHT Lens
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
Environment=NODE_ENV=production
EnvironmentFile=-${APP_DIR}/.env
ExecStart=${NODE_BIN} ${APP_DIR}/app.mjs
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

sudo systemctl daemon-reload
sudo systemctl enable "${APP_NAME}"
sudo systemctl restart "${APP_NAME}"
sudo systemctl --no-pager --full status "${APP_NAME}"
