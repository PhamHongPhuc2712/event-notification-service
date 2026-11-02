#!/usr/bin/env bash
# End-to-end smoke test against a running stack:
#   docker compose up, api on :8080, worker on :8081.
# Sends one email notification and waits until it is SENT and visible in Mailpit.
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MAILPIT_URL="${MAILPIT_URL:-http://localhost:8025}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"

subject="Smoke test $(date +%s)-$RANDOM"
response=$(curl -sf -X POST "$API_URL/notifications" \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"smoke-user\",\"channel\":\"EMAIL\",\"recipient\":\"smoke@example.com\",\"subject\":\"$subject\",\"body\":\"Hello from the smoke test\"}")
id=$(printf '%s' "$response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
echo "Created notification $id"

status=""
for _ in $(seq 1 "$TIMEOUT_SECONDS"); do
  status=$(curl -sf "$API_URL/notifications/$id" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
  [ "$status" = "SENT" ] && break
  [ "$status" = "FAILED" ] && { echo "FAIL: notification marked FAILED"; exit 1; }
  sleep 1
done
[ "$status" = "SENT" ] || { echo "FAIL: status is '$status' after ${TIMEOUT_SECONDS}s"; exit 1; }
echo "Status is SENT"

if curl -sf "$MAILPIT_URL/api/v1/messages?limit=200" | grep -qF "$subject"; then
  echo "PASS: email is in Mailpit"
else
  echo "FAIL: email not found in Mailpit"; exit 1
fi
