# Event-Driven Notification Service

A multi-channel notification service. Other applications ask it to notify a user; it picks
the channel, respects user preferences and rate limits, and delivers every notification
exactly once from the user's point of view, even when workers, the broker, or providers fail.

> **Status:** Weeks 1–2 done — email notifications flow end to end through the outbox, Kafka and the worker. See [docs/PLAN.md](docs/PLAN.md) for the roadmap
> and [docs/DESIGN.md](docs/DESIGN.md) for the design.

## Stack

Java 21 · Spring Boot 4.1 · Kafka 4.3 (KRaft) · PostgreSQL 16 · Redis 7 · Docker Compose ·
Prometheus · Grafana · JUnit 5 · Testcontainers · k6

## Modules

| Module | Port | Purpose |
|---|---|---|
| `api` | 8080 | Accepts notifications, writes them and an outbox row in one transaction |
| `worker` | 8081 | Kafka consumers that deduplicate, rate-limit and deliver |
| `mock-sms` | 8082 | Fake SMS provider that injects timeouts, errors and slow responses |
| `infra` | — | Docker Compose stack, Prometheus and Grafana config |

## Quick start

Prerequisites: Java 21 and Docker Desktop.

```bash
# 1. Configure local secrets (once)
cp infra/.env.example infra/.env        # then edit the passwords

# 2. Start infrastructure: Kafka, Postgres, Redis, Mailpit
docker compose -f infra/docker-compose.yml up -d

# Optional: also start Prometheus and Grafana
docker compose -f infra/docker-compose.yml --profile observability up -d

# 3. Build and test everything
./mvnw verify

# 4. Run a service (one terminal each)
./mvnw -pl api spring-boot:run
./mvnw -pl worker spring-boot:run
./mvnw -pl mock-sms spring-boot:run
```

On Windows PowerShell, use `.\mvnw.cmd` instead of `./mvnw`.

## Python helper scripts

Python is only used for helper scripts such as the Week 7 load-test verifier.

```bash
python -m venv .venv                      # once
source .venv/Scripts/activate             # Git Bash
.venv\Scripts\Activate.ps1                # PowerShell
pip install -r requirements.txt
```

## Try it

```bash
curl -i -X POST http://localhost:8080/notifications   -H "Content-Type: application/json"   -d '{"userId":"user-42","channel":"EMAIL","recipient":"alice@example.com","subject":"Your order shipped","body":"It arrives Friday."}'
# 202 Accepted, Location: /notifications/{id}, status QUEUED

curl http://localhost:8080/notifications/{id}
# status becomes SENT within a second; the email shows up at http://localhost:8025
```

Invalid requests return `400` with an RFC 9457 problem body listing each invalid field.

Retries are safe with an `Idempotency-Key` header (scoped per `userId`). Repeating the request
returns the same notification with `Idempotent-Replayed: true`; reusing the key with a different
body returns `422`.

```bash
curl -i -X POST http://localhost:8080/notifications -H "Content-Type: application/json"   -H "Idempotency-Key: order-42-shipped"   -d '{"userId":"user-42","channel":"EMAIL","recipient":"alice@example.com","subject":"Your order shipped","body":"It arrives Friday."}'
```

End-to-end smoke test, with infrastructure, `api` and `worker` running:

```bash
bash scripts/smoke-test.sh
```

## Local URLs

| What | URL |
|---|---|
| API health | http://localhost:8080/actuator/health |
| Mailpit inbox | http://localhost:8025 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## Stopping

```bash
docker compose -f infra/docker-compose.yml --profile observability down       # keep data
docker compose -f infra/docker-compose.yml --profile observability down -v    # wipe data
```
