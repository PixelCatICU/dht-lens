#!/usr/bin/env sh
set -eu

: "${CAPROVER_APP:?CAPROVER_APP is required}"

if ! command -v caprover >/dev/null 2>&1; then
  echo "caprover CLI is not installed. Install it with: npm install -g caprover" >&2
  exit 1
fi

caprover deploy --appName "$CAPROVER_APP"
