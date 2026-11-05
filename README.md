# Event-Driven Notification Service

A multi-channel notification service. Other applications ask it to notify a user; it picks
the channel, respects user preferences and rate limits, and delivers every notification
exactly once from the user's point of view, even when workers, the broker, or providers fail.

> **Status:** email and SMS flow end to end through the outbox, Kafka and the workers, with
> idempotency keys, retry topics, a dead-letter topic with replay, and a chaos test that kills
> workers mid-run and loses or duplicates nothing. See [docs/DESIGN.md](docs/DESIGN.md) for the design.

## Stack

Java 21 · Spring Boot 4.1 · Kafka 4.3 (KRaft) · PostgreSQL 16 · Redis 7 · Docker Compose ·
Prometheus · Grafana · JUnit 5 · Testcontainers · k6

## Modules

| Module | Port | Purpose |
|---|---|---|
| `api` | 8080 | Accepts notifications, writes them and an outbox row in one transaction |
| `worker` | 8081 | Kafka consumers that claim, deliver and retry; also the dead-letter replay tool |
| `mock-sms` | 8082 | Fake SMS provider that injects timeouts, errors and slow responses |
| `chaos-test` | — | Kills workers and the API mid-run; checks nothing is lost or sent twice |
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

Python is only used for helper scripts such as the load-test verifier.

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

SMS works the same way, with an E.164 phone number and no subject. Run `mock-sms` too; it fails,
times out or answers slowly on purpose, and the worker retries until it gets through:

```bash
curl -i -X POST http://localhost:8080/notifications -H "Content-Type: application/json" \
  -d '{"userId":"user-42","channel":"SMS","recipient":"+6591234567","body":"Your code is 123456"}'
curl http://localhost:8082/v1/messages/stats    # what the fake provider received
```

Invalid requests return `400` with an RFC 9457 problem body listing each invalid field.

Retries are safe with an `Idempotency-Key` header (scoped per `userId`). Repeating the request
returns the same notification with `Idempotent-Replayed: true`; reusing the key with a different
body returns `422`.

```bash
curl -i -X POST http://localhost:8080/notifications -H "Content-Type: application/json"   -H "Idempotency-Key: order-42-shipped"   -d '{"userId":"user-42","channel":"EMAIL","recipient":"alice@example.com","subject":"Your order shipped","body":"It arrives Friday."}'
```

## User preferences

Users can turn channels off and set quiet hours in their own time zone. Preferences are checked
when a notification is delivered, so a change also applies to notifications already queued.
A notification on a disabled channel ends as `SUPPRESSED`; one that arrives during quiet hours
becomes `DEFERRED` with a `deliverAfter` time, and is sent when the quiet hours end.

```bash
curl -X PUT http://localhost:8080/users/user-42/preferences -H "Content-Type: application/json" \
  -d '{"allowedChannels":["EMAIL"],"quietHours":{"start":"22:00","end":"07:00","timeZone":"Asia/Singapore"}}'
curl http://localhost:8080/users/user-42/preferences   # every channel, no quiet hours, if never set
```

## Rate limits

Each user has a token bucket per channel in Redis: by default a burst of 5 SMS, then 5 a
minute, and a burst of 20 emails, then 20 a minute. A notification over the limit is not
dropped: it reserves the next free slot and becomes `DEFERRED` until then. Only one that would
wait more than an hour is `SUPPRESSED` with `RATE_LIMITED`. Limits are set under
`notifications.rate-limit` in `worker/src/main/resources/application.yml`.

## Failures, retries and replay

A send that fails for a reason a retry may fix moves through `<topic>-retry-0` … `-retry-4`
with growing, jittered delays (about 2s, 6s, 18s, 54s, 60s). After the last retry it lands in
`<topic>-dlt` and the notification is marked `FAILED` with `RETRIES_EXHAUSTED`. Change the fake
provider's fault rates at runtime to see it:

```bash
curl -X PUT http://localhost:8082/admin/faults -H "Content-Type: application/json" \
  -d '{"failureRate":1,"timeoutRate":0,"slowRate":0,"timeoutDelay":"PT10S","slowDelay":"PT1S"}'
```

Once the cause is fixed, replay the dead letters. The tool requeues each notification, sends
it through the main topic again, and exits:

```bash
./mvnw -pl worker spring-boot:run -Dspring-boot.run.profiles=replay \
  -Dspring-boot.run.arguments=--topic=notifications.sms-dlt
```

## Tests

```bash
./mvnw verify            # unit and integration tests (Testcontainers; needs Docker)
./mvnw -Pchaos verify    # also the chaos test: about 3 minutes, report in chaos-test/target/chaos-report.md
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
| Mock SMS stats | http://localhost:8082/v1/messages/stats |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## Stopping

```bash
docker compose -f infra/docker-compose.yml --profile observability down       # keep data
docker compose -f infra/docker-compose.yml --profile observability down -v    # wipe data
```
