#!/usr/bin/env bash

set -euo pipefail

# Ensure CapRover CLI is installed
if ! command -v caprover &> /dev/null; then
    echo "CapRover CLI is not installed. Please install it first:"
    echo "npm install -g caprover"
    exit 1
fi

APP_NAME="${CAPROVER_APP:-dht-lens}"
BRANCH="${CAPROVER_BRANCH:-main}"

DEPLOY_ARGS=(
    --caproverApp "$APP_NAME"
    --branch "$BRANCH"
)

if [[ -n "${CAPROVER_NAME:-}" ]]; then
    DEPLOY_ARGS+=(--caproverName "$CAPROVER_NAME")
fi

if [[ -n "${CAPROVER_URL:-}" ]]; then
    DEPLOY_ARGS+=(--caproverUrl "$CAPROVER_URL")
fi

if [[ -n "${CAPROVER_PASSWORD:-}" ]]; then
    DEPLOY_ARGS+=(--caproverPassword "$CAPROVER_PASSWORD")
fi

if [[ -n "${CAPROVER_APP_TOKEN:-}" ]]; then
    DEPLOY_ARGS+=(--appToken "$CAPROVER_APP_TOKEN")
fi

# Deploy to CapRover
echo "Deploying $APP_NAME to CapRover from branch $BRANCH..."
caprover deploy "${DEPLOY_ARGS[@]}"
