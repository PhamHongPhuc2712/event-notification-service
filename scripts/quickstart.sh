#!/usr/bin/env bash
# One command to run everything in Docker: infrastructure, the three services and the
# monitoring stack. Creates infra/.env with random secrets the first time.
#
#   bash scripts/quickstart.sh          # start (builds the images on the first run)
#   bash scripts/quickstart.sh down     # stop, keeping data
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f infra/docker-compose.yml --profile app --profile observability)

if [ "${1:-}" = down ]; then
  "${COMPOSE[@]}" down
  exit 0
fi

random() { head -c "$1" /dev/urandom | base64 | tr -d '/+=\n'; }

if [ ! -f infra/.env ]; then
  cat > infra/.env <<EOF
# Created by scripts/quickstart.sh. Local only: infra/.env is gitignored.
POSTGRES_DB=notifications
POSTGRES_USER=notifications
POSTGRES_PASSWORD=$(random 18)
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=$(random 18)
WEBHOOK_SIGNING_SECRET=whsec_$(head -c 32 /dev/urandom | base64 | tr -d '\n')
EOF
  echo "Created infra/.env with random passwords."
elif ! grep -q '^WEBHOOK_SIGNING_SECRET=whsec_' infra/.env; then
  echo "infra/.env needs WEBHOOK_SIGNING_SECRET=whsec_<base64>; see infra/.env.example" >&2
  exit 1
fi

"${COMPOSE[@]}" up -d --build

echo "Waiting for the services to be healthy..."
for service in ens-api ens-worker ens-mock-sms; do
  for _ in $(seq 1 90); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$service" 2>/dev/null)" = healthy ] && break
    sleep 2
  done
  status=$(docker inspect -f '{{.State.Health.Status}}' "$service" 2>/dev/null || echo missing)
  [ "$status" = healthy ] || { echo "$service is $status; see: docker logs $service" >&2; exit 1; }
done

cat <<'EOF'

Everything is running.
  API            http://localhost:8080/notifications
  Mailpit inbox  http://localhost:8025
  Grafana        http://localhost:3000   (user and password in infra/.env)
  Prometheus     http://localhost:9090

Send a notification:
  curl -i -X POST http://localhost:8080/notifications -H 'Content-Type: application/json' \
    -d '{"userId":"user-42","channel":"EMAIL","recipient":"alice@example.com","subject":"Hello","body":"It works."}'

Stop with: bash scripts/quickstart.sh down
EOF
