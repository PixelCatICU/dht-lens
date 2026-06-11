#!/usr/bin/env bash

set -euo pipefail

if ! command -v docker &> /dev/null; then
    echo "Docker is not installed or not available in PATH."
    exit 1
fi

docker compose up -d --build
