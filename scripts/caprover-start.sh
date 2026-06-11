#!/usr/bin/env sh
set -eu

: "${LIBSQL_DATABASE_URL:?LIBSQL_DATABASE_URL is required}"
: "${LIBSQL_AUTH_TOKEN:?LIBSQL_AUTH_TOKEN is required}"

dht-lens migrate
exec dht-lens crawl --print
