#!/usr/bin/env bash
# Chaos run: a load test while workers are killed, Kafka restarts, the network to Kafka and
# Postgres slows down, and the API is killed, then the usual check that every accepted
# notification was delivered exactly once.
#
# Starts its own api, two workers and mock-sms from the packaged jars, connected to Kafka,
# Postgres and Redis through Toxiproxy, and stops them at the end. Needs:
#   ./mvnw -q package -DskipTests
#   docker compose -f infra/docker-compose.yml --profile observability --profile chaos up -d
# and nothing else listening on ports 8080, 8081, 8082 or 8091.
#
#   RATE=300 DURATION=150s bash loadtest/chaos.sh
#
# The timeline below assumes DURATION of at least 150s. Everything it does is logged with a
# timestamp to loadtest/results/<run id>/chaos.log.
set -euo pipefail

cd "$(dirname "$0")/.."
export RUN_ID="${RUN_ID:-chaos-$(date +%Y%m%d-%H%M%S)}"
export RATE="${RATE:-300}" DURATION="${DURATION:-150s}"
RESULTS="loadtest/results/$RUN_ID"
TOXIPROXY="${TOXIPROXY:-http://localhost:8474}"
mkdir -p "$RESULTS"

for jar in api worker mock-sms; do
  [ -f "$jar/target/$jar-0.0.1-SNAPSHOT.jar" ] || { echo "Missing $jar jar; run ./mvnw -q package -DskipTests" >&2; exit 2; }
done
for port in 8080 8081 8082 8091; do
  if curl -s -m 1 "localhost:$port" > /dev/null 2>&1; then echo "Port $port is in use; stop the services first" >&2; exit 2; fi
done
curl -sf -m 3 "$TOXIPROXY/proxies" > /dev/null || { echo "Toxiproxy is not running (compose --profile chaos)" >&2; exit 2; }

# Through the proxies, so the run can slow them down. A dead worker's partitions move after 10s.
export KAFKA_BOOTSTRAP_SERVERS=localhost:19094 POSTGRES_PORT=15432 REDIS_PORT=16379 KAFKA_SESSION_TIMEOUT_MS=10000

started=$(date +%s)
log() { echo "t+$(( $(date +%s) - started ))s $*" | tee -a "$RESULTS/chaos.log"; }

start() { # name module port
  # Appended, so a restarted service keeps the log of the one that was killed.
  nohup java -Xmx1g -jar "$2/target/$2-0.0.1-SNAPSHOT.jar" --server.port="$3" >> "$RESULTS/$1.log" 2>&1 &
  echo $! > "$RESULTS/$1.pid"
}
await_healthy() { # port
  for _ in $(seq 1 120); do
    curl -s -m 2 "localhost:$1/actuator/health" | grep -q '"UP"' && return 0
    sleep 1
  done
  echo "Service on port $1 did not become healthy" >&2; return 1
}
kill9() { # name
  kill -9 "$(cat "$RESULTS/$1.pid")" 2>/dev/null || true
  log "killed $1 (kill -9)"
}
stop_all() {
  for pid in "$RESULTS"/*.pid; do [ -e "$pid" ] && kill "$(cat "$pid")" 2>/dev/null || true; done
}
trap stop_all EXIT

toxic() { # proxy action
  case $2 in
    slow)
      for stream in upstream downstream; do
        curl -sf -X POST "$TOXIPROXY/proxies/$1/toxics" -H 'Content-Type: application/json' \
          -d "{\"name\":\"latency-$stream\",\"type\":\"latency\",\"stream\":\"$stream\",\"attributes\":{\"latency\":200,\"jitter\":100}}" > /dev/null
      done ;;
    heal)
      for stream in upstream downstream; do
        curl -sf -X DELETE "$TOXIPROXY/proxies/$1/toxics/latency-$stream" > /dev/null || true
      done ;;
  esac
}

timeline() {
  sleep 20; kill9 worker-1
  sleep 5;  start worker-1 worker 8081; log "restarted worker-1"
  sleep 15; log "restarting Kafka"; docker restart ens-kafka > /dev/null; log "Kafka restarted"
  sleep 15; toxic kafka slow; toxic postgres slow; log "added 200 ms ± 100 ms latency each way to Kafka and Postgres"
  sleep 20; toxic kafka heal; toxic postgres heal; log "removed the latency"
  sleep 5;  kill9 worker-2
  sleep 5;  start worker-2 worker 8091; log "restarted worker-2"
  sleep 15; kill9 api
  sleep 3;  start api api 8080; log "restarting api"; await_healthy 8080; log "api healthy again"
}

start mock-sms mock-sms 8082
start api api 8080
start worker-1 worker 8081
start worker-2 worker 8091
for port in 8082 8080 8081 8091; do await_healthy "$port"; done
toxic kafka heal; toxic postgres heal   # in case an earlier run was interrupted
log "all services healthy; starting load at ${RATE}/s for ${DURATION}"

timeline &
timeline_pid=$!
# The provider fails 5% of requests at once, so retries run alongside the chaos. Its slow and
# timed-out responses are left to the chaos-test module: six SMS threads waiting seconds per
# request cannot keep up with this load, and the backlog would hide what the chaos does.
SMS_FAULTS="${SMS_FAULTS:-{\"failureRate\":0.05,\"timeoutRate\":0,\"slowRate\":0,\"timeoutDelay\":\"PT6S\",\"slowDelay\":\"PT1S\"\}}" \
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-900}" bash loadtest/run.sh || status=$?
# Only the timeline: a plain wait would also wait for the services, which never exit.
wait "$timeline_pid" || true
log "run finished"
exit "${status:-0}"
