# Event-Driven Notification Service: Project Plan

**Goal:** a multi-channel notification service that other apps call to notify a user.
It picks the channel, respects user preferences and rate limits, and delivers every
notification exactly once, even when parts of the system crash.

**Timeline:** 8 weeks plus a setup week, at roughly 10–15 hours per week.
**Cost:** free. Everything runs locally in Docker. No cloud account needed.

---

## Final decisions

| Decision | Choice | Reason |
|---|---|---|
| Language | Java 21 | You already know Java; virtual threads available |
| Framework | Spring Boot 4.1, Spring Kafka | Built-in retry topics and dead-letter publishing |
| Build tool | Maven | Simple, widely used |
| Broker | Apache Kafka 4.3 in KRaft mode, single Docker image | Partitions, ordering, consumer groups, offsets |
| Database | PostgreSQL 16 | Notifications, outbox, dedupe table, delivery status |
| Rate limiting | Redis 7 with a Lua token bucket | Atomic per-user limits across workers |
| Email | Mailpit | Fake inbox viewable in the browser |
| SMS | Your own mock provider with fault injection | Proves retry and failure handling |
| Metrics | Micrometer, Prometheus, Grafana | Consumer lag, latency, errors |
| Testing | JUnit 5, Testcontainers, k6, Toxiproxy | Integration, load and chaos testing |
| CI | GitHub Actions | Build and test on every push |
| Deployment | None required; optional cheap VPS with Docker Compose | Keeps it free |

**Delivery guarantee:** at-least-once delivery from Kafka plus idempotent consumers,
which gives effectively-once delivery to the user. The design doc explains why true
exactly-once delivery to an external provider is impossible.

## Architecture

```
Client app ──POST /notifications (Idempotency-Key)──► API service
                                                        │ notification + outbox row,
                                                        ▼ one transaction
                                                     PostgreSQL
                                                        │ outbox publisher
                                                        ▼
                                          Kafka topics, partitioned by user ID
                                     ┌──────────────┼──────────────┐
                                     ▼              ▼              ▼
                                   email         webhook        sms (mock)
                                     │  each with retry topics + dead-letter topic
                                     ▼
                         Worker service (Spring Kafka consumers)
                         - dedupe via unique constraint in Postgres
                         - per-user rate limit via Redis
                         - commit offset only after processing
                                     ▼
                         Mailpit / signed webhook / flaky mock provider
                                     ▼
                         Delivery status in PostgreSQL ──► GET /notifications/{id}
```

## Repository layout

```
notification-service/
  api/          Spring Boot API + outbox publisher
  worker/       Spring Boot Kafka consumers
  mock-sms/     Fake SMS provider with fault injection
  loadtest/     k6 scripts + verifier script
  infra/        docker-compose.yml, Grafana dashboards, Prometheus config
  docs/         PLAN.md, DESIGN.md, RESULTS.md
```

## Target numbers

Aim for these, then report what you actually measure.

| Metric | Target |
|---|---|
| Sustained throughput on one laptop | 2,000+ notifications/sec |
| End-to-end p99 latency at that load | under 200 ms |
| Lost notifications in chaos runs | 0 |
| Duplicate deliveries in chaos runs | 0 |
| Test coverage | 80%+ |

---

## Week 0: design and setup
- [x] Write a one-page `docs/DESIGN.md`: goals, architecture, delivery guarantee
- [x] Create the repo and folder layout above
- [x] `docker-compose.yml` starts Kafka, Postgres, Redis, Mailpit, Prometheus, Grafana
- [x] GitHub Actions builds and runs tests on every push
- **Done when:** `docker compose up` starts everything and CI is green

## Weeks 1–2: happy path
- [x] `POST /notifications` and `GET /notifications/{id}`
- [x] Postgres schema with Flyway migrations
- [x] Outbox publisher sends to Kafka, keyed by user ID
- [x] Email worker consumes and sends to Mailpit
- [x] Status moves through QUEUED, SENT, FAILED
- **Done when:** a Testcontainers test sends a notification end to end

## Weeks 3–4: reliability (the core of the project)
- [x] API idempotency: repeated `Idempotency-Key` returns the original notification
- [x] Transactional outbox: notification and outbox row commit together
- [x] Consumer dedupe: insert message ID into a table with a unique constraint before sending
- [x] Commit Kafka offsets only after processing succeeds
- [x] Retry topics with exponential backoff and jitter
- [x] Dead-letter topic plus a replay tool
- [x] Mock SMS provider that randomly times out, fails, or responds slowly
- **Done when:** killing workers mid-run ends with zero lost and zero duplicate deliveries
  — done: `./mvnw -Pchaos verify`, results in [DESIGN.md](DESIGN.md#chaos-test)

## Week 5: features
- [x] User preferences: allowed channels, quiet hours
- [x] Per-user rate limiting with a Redis token bucket in a Lua script
- [x] High-priority topic so password resets skip the marketing backlog
- [x] Webhook channel with HMAC-signed requests
- **Done when:** every feature has tests, including concurrent tests for the rate limiter

## Week 6: observability
- [x] Export metrics with Micrometer to Prometheus
- [x] Grafana dashboard: consumer lag, delivery latency, retries, dead letters, errors
- [x] Structured JSON logs with a notification ID on every line
- **Done when:** you can watch a load test happen live on the dashboard
  — done: dashboard *Notification service* in Grafana; see [README](../README.md#observability).
  The k6 load test itself comes in Week 7.

## Week 7: load and chaos testing
- [x] k6 load test; find the bottleneck, fix it, measure again
- [x] During load: kill workers, restart Kafka, slow the network with Toxiproxy
- [x] Verifier script compares accepted vs delivered and counts lost or duplicated
- [x] Record everything in `docs/RESULTS.md`
- **Done when:** a results table shows throughput, p99 latency, consumer lag, lost and duplicate counts
  — done: [RESULTS.md](RESULTS.md). About 430 a second sustained on the laptop, bound by the
  disk's WAL flush rate; zero lost in every run, two duplicate emails under `kill -9`.

## Week 8: write-up
- [ ] README: architecture diagram, one-command quick start, benchmark results
- [ ] `DESIGN.md` tradeoffs: why Kafka, why dedupe in Postgres, what "exactly-once" means here
- [ ] "Hardest bug" section
- [ ] Final resume bullet using your real numbers

---

## Risks

| Risk | Mitigation |
|---|---|
| Kafka is new and confusing | Spend two days on Kafka basics before Week 1 |
| Docker uses too much memory on a laptop | Run a single Kafka broker; give Docker 6–8 GB |
| Scope grows | Weeks 3–4 matter most; cut Week 5 features before cutting reliability |
| Flaky tests from timing | Use Testcontainers and await conditions instead of sleeps |

## Interview questions to be ready for
1. What happens if a worker crashes after sending but before committing its offset?
2. How do partitions keep one user's notifications in order, and what happens when you add consumers?
3. How does your rate limiter behave when two workers handle the same user at once?
4. What was your load-test bottleneck, and how did you fix it?
5. How would you scale this to 100 times the traffic?

## Resume bullet template

```
Built an event-driven notification service in Java and Kafka delivering <N> msgs/sec at
p99 < <X> ms, with zero lost or duplicate deliveries across <M> chaos-tested events using
a transactional outbox, idempotent consumers, and retry/dead-letter topics.
```
