#!/usr/bin/env sh
set -eu

: "${LIBSQL_DATABASE_URL:?LIBSQL_DATABASE_URL is required}"
: "${LIBSQL_AUTH_TOKEN:?LIBSQL_AUTH_TOKEN is required}"

node /app/js/app.mjs migrate
exec node /app/js/app.mjs crawl --print
