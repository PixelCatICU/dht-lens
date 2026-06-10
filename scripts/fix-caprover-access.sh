#!/usr/bin/env sh
set -eu

SERVICE_NAME="${CAPROVER_SERVICE_NAME:-captain-captain}"
CAPTAIN_HOST="${CAPROVER_PANEL_HOST:-captain.vlist.cyou}"
DNSRR_TIMEOUT="${CAPROVER_DNSRR_TIMEOUT_SECS:-15}"

echo "[1/5] ensure panel service uses supported Docker API version"
docker service update \
  --env-add DOCKER_API_VERSION=1.40 \
  --update-order stop-first \
  --force "$SERVICE_NAME" >/dev/null

echo "[2/5] switch panel service to DNSRR endpoint mode"
docker service update \
  --endpoint-mode dnsrr \
  --update-order stop-first \
  --force "$SERVICE_NAME" >/dev/null

echo "[3/5] wait for rollout"
sleep 8

echo "[4/5] show service endpoint"
docker service inspect "$SERVICE_NAME" --format '{{json .Endpoint.Spec}}'

echo "[5/5] health check"
timeout "$DNSRR_TIMEOUT" curl -k -sSf -I "https://$CAPTAIN_HOST/checkhealth" || {
  echo "panel check failed"
  docker service logs --tail 80 captain-nginx
  exit 1
}
echo "panel appears healthy"

