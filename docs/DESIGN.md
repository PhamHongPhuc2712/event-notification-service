# Design

> Week 0 draft. Expand the tradeoffs section as you build.

## Problem

Other applications need to notify their users through email, SMS and webhooks.
They want to fire one request and trust that the user gets the message exactly once,
in order, without being spammed, even when parts of the system fail.

## Goals

- Accept a notification request and return quickly with an ID and status.
- Deliver each accepted notification to the user **exactly once from the user's point of view**.
- Keep each user's notifications in order.
- Respect user preferences, quiet hours and per-user rate limits.
- Survive worker crashes, broker restarts, and a slow or failing provider.

## Non-goals

- Real SMS or push providers. A mock provider stands in for SMS.
- Multi-region deployment.
- A user-facing UI.

## Delivery guarantee

Kafka gives **at-least-once** delivery: a message can be redelivered after a crash.
Workers are **idempotent**: before sending, a worker records the message ID in a table
with a unique constraint, and skips any ID it has already handled.
At-least-once plus idempotent processing gives **effectively-once** delivery to the user.

True exactly-once delivery to an external provider is impossible. If a worker crashes
after the provider accepts the message but before the worker records success, the system
cannot know whether the send happened. Open question to resolve in Weeks 3–4: which side
of that gap to choose, and how provider-side idempotency keys narrow it.

## Architecture

```
Client ──POST /notifications (Idempotency-Key)──► API ──► Postgres (notification + outbox row)
                                                              │ outbox publisher
                                                              ▼
                                               Kafka, partitioned by user ID
                                                              ▼
                                   Worker: dedupe ─► rate limit ─► send ─► record status
                                                              ▼
                                          Mailpit │ signed webhook │ mock SMS
```

## Key decisions

| Decision | Choice | Why | Alternatives considered |
|---|---|---|---|
| Broker | Kafka | Per-key ordering via partitions, replay, consumer groups | RabbitMQ, SQS |
| Publishing | Transactional outbox | Avoids the dual-write problem between Postgres and Kafka | Publish directly after commit |
| Dedupe store | Postgres unique constraint | Durable and transactional with the status update | Redis with TTL |
| Rate limiting | Redis token bucket in Lua | Atomic across many workers | In-memory per worker |

## Tradeoffs and open questions

- **Outbox polling vs. change data capture.** A 200 ms poller is simple and needs no extra
  infrastructure, but adds up to one poll interval of latency and a small constant query load.
  Debezium would stream changes from the Postgres log instead, at the cost of running Kafka Connect.
- **Ordering with more than one publisher.** `FOR UPDATE SKIP LOCKED` lets several API instances
  publish in parallel, but then two rows for the same user can be sent out of order. With a single
  publisher, order is preserved. Options if this matters: one publisher elected via an advisory
  lock, or publishers that each own a subset of users. Even one publisher has a smaller gap:
  identity IDs are assigned at insert but become visible at commit, so two concurrent requests
  for the same user can commit in the opposite order of their IDs and be published out of order.
  Fixing that needs a per-user sequence number that the worker checks.
- **The outbox publishes at least once.** Rows are marked published only after Kafka acknowledges
  the whole batch. A crash between the ack and the commit republishes the batch, so consumers
  must be idempotent.
- **Offsets commit per poll batch.** The worker uses Spring Kafka's batch ack mode: offsets are
  committed after the listener has processed every record from a poll. A crash mid-batch
  redelivers the processed records too, which is safe only because delivery is idempotent.
- **The Week 1–2 duplicate check is not race-proof.** The worker reads the status and only sends
  when it is QUEUED, which stops ordinary redeliveries. Two consumers could still both read
  QUEUED during a rebalance. Weeks 3–4 replace this with an atomic claim.
- **Transient errors retry in place.** The worker retries a failing record with exponential
  backoff for about five minutes, which keeps per-user order but blocks that partition. Weeks 3–4
  move retries to separate retry topics so one bad record cannot stall a partition.
- **Temporary versus permanent email failures.** Only a refusal from the mail server, such as
  an unknown mailbox, marks a notification FAILED. Connection errors and timeouts are retried,
  because marking them FAILED would make the status final and block any later retry.
- **Public failure reasons are codes.** The API returns `RECIPIENT_REJECTED`, never the raw SMTP
  error, which can reveal internal hostnames and ports. The full error goes to the logs.
- **Crash between sending and recording.** If the database write fails after the email was sent,
  the retry sends the email again, and a record skipped after five minutes stays QUEUED forever.
  Planned: the Weeks 3–4 claim table, plus a reconciler for notifications stuck in QUEUED.
- **The outbox grows forever.** Published rows are never deleted. The partial index keeps polling
  fast, but a retention job should delete rows published more than a few days ago.

## Hardest bug

- _Fill in during Weeks 3–7._
