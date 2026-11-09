#!/usr/bin/env bash
# Runs one load test, then verifies that every accepted notification was delivered exactly once.
#
# Needs the infrastructure (with --profile observability for the lag numbers) and the api,
# worker and mock-sms services running. Results go to loadtest/results/<run id>/.
#
#   RATE=1000 DURATION=60s bash loadtest/run.sh
#
# Environment:
#   RATE, DURATION, USERS, SMS_SHARE, HIGH_SHARE  passed to the k6 script (see notifications.js)
#   RUN_ID       defaults to a timestamp
#   SMS_FAULTS   1 keeps the mock provider's failures, timeouts and slow responses; a JSON object
#                sets them, as PUT /admin/faults takes it; the default turns them off, so the run
#                measures the service rather than the fake provider
#   DRAIN_TIMEOUT seconds the verifier waits for the backlog to empty (default 600)
#   K6_HOOK      a command run in the background while k6 runs, such as loadtest/chaos.sh
set -euo pipefail

cd "$(dirname "$0")/.."
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RESULTS="loadtest/results/$RUN_ID"
MOCK_SMS="${MOCK_SMS:-http://localhost:8082}"
MAILPIT="${MAILPIT:-http://localhost:8025}"
PYTHON="${PYTHON:-.venv/bin/python}"
mkdir -p "$RESULTS"

for url in http://localhost:8080 http://localhost:8081 "$MOCK_SMS"; do
  curl -sf -m 3 "$url/actuator/health" > /dev/null || { echo "Not running: $url" >&2; exit 2; }
done

# Start from an empty provider and inbox, so the counts belong to this run only.
curl -sf -X DELETE "$MOCK_SMS/v1/messages" > /dev/null
curl -sf -X DELETE "$MAILPIT/api/v1/messages" > /dev/null
case "${SMS_FAULTS:-0}" in
  1) ;;                                   # keep the provider's own fault settings
  \{*) curl -sf -X PUT "$MOCK_SMS/admin/faults" -H 'Content-Type: application/json' -d "$SMS_FAULTS" > /dev/null ;;
  *) curl -sf -X PUT "$MOCK_SMS/admin/faults" -H 'Content-Type: application/json' \
       -d '{"failureRate":0,"timeoutRate":0,"slowRate":0,"timeoutDelay":"PT10S","slowDelay":"PT1S"}' > /dev/null ;;
esac

echo "Run $RUN_ID: ${RATE:-500}/s for ${DURATION:-60s}"
hook_pid=""
if [ -n "${K6_HOOK:-}" ]; then
  RUN_ID="$RUN_ID" RESULTS="$RESULTS" bash -c "$K6_HOOK" > "$RESULTS/hook.log" 2>&1 &
  hook_pid=$!
fi

# k6 runs in Docker on the host network, so localhost is the API.
docker run --rm -i --network host --user "$(id -u):$(id -g)" \
  -v "$PWD/loadtest/results:/results" \
  -e RUN_ID="$RUN_ID" -e RATE="${RATE:-500}" -e DURATION="${DURATION:-60s}" \
  -e USERS="${USERS:-50000}" -e SMS_SHARE="${SMS_SHARE:-0.5}" -e HIGH_SHARE="${HIGH_SHARE:-0.1}" \
  grafana/k6:1.4.2 run --quiet - < loadtest/notifications.js | tee "$RESULTS/k6.log" || true

if [ -n "$hook_pid" ]; then
  wait "$hook_pid" || echo "The hook exited with an error; see $RESULTS/hook.log" >&2
fi

"$PYTHON" loadtest/verify.py --run-id "$RUN_ID" --timeout "${DRAIN_TIMEOUT:-600}" | tee "$RESULTS/report.md"
