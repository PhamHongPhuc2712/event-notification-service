# Design

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
Workers are **idempotent**: before sending, a worker claims the notification in a table
keyed by its ID, and skips any notification that is no longer QUEUED.
At-least-once plus idempotent processing gives **effectively-once** delivery to the user.

True exactly-once delivery to an external provider is impossible. If a worker crashes
after the provider accepts the message but before the worker records success, the system
cannot know whether the send happened. The system chooses the **resend** side of that gap:
losing a password-reset code is worse than sending it twice. Where the provider supports an
idempotency key, as the SMS provider does, the resend is harmless: the worker sends the
notification ID as the key, and the provider accepts each key once. Email over SMTP has no
such key, so an email can still, rarely, arrive twice.

The chaos test checks all of this end to end; see [Chaos test](#chaos-test).

## Architecture

```
Client ──POST /notifications (Idempotency-Key)──► API ──► Postgres (notification + outbox row)
                                                              │ outbox publisher
                                                              ▼
                                               Kafka, partitioned by user ID
                                                              ▼
                                   Worker: claim ─► send ─► record status
                                       │ transient failure             ▼
                                       ▼                     Mailpit │ mock SMS
                         <topic>-retry-0 … -retry-N (jittered delays)
                                       │ out of retries
                                       ▼
                         <topic>-dlt ─► marked FAILED ─► replay tool ─► <topic>
```

## Key decisions

| Decision | Choice | Why | Alternatives considered |
|---|---|---|---|
| Broker | Kafka | Per-key ordering via partitions, replay, consumer groups | RabbitMQ, SQS |
| Publishing | Transactional outbox | Avoids the dual-write problem between Postgres and Kafka | Publish directly after commit |
| Dedupe store | Postgres unique constraint | Durable and transactional with the status update | Redis with TTL |
| Rate limiting | Redis token bucket in Lua, with reserved slots | Atomic across many workers; a burst drains without retry storms | In-memory per worker; Redis INCR fixed windows |
| Quiet hours | DEFERRED rows released by a scheduled job | Kafka has no per-message delay of hours | Delay topics per hour; consumer pause |
| Retries | Retry topics with per-record jitter, then a dead-letter topic | A failing record does not stall its partition | Blocking retries in place |
| SMS duplicates | Provider idempotency key = notification ID | A timed-out send can be retried safely | Never retry a timeout (loses messages) |

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
- **Offsets commit only after a record is settled.** The worker uses Spring Kafka's batch ack
  mode: offsets are committed after the listener has handled every record from a poll. A record
  counts as handled once it is sent, failed permanently, or published to a retry topic, so a
  crash never skips an unsettled record. A crash mid-batch redelivers the handled records too,
  which is safe only because delivery is idempotent. The chaos test kills workers with the
  equivalent of `kill -9`, which commits nothing, and still loses no notification.
- **Idempotency keys are stored with the notification, in the same transaction.** The API
  inserts `(user_id, key)` into `idempotency_keys` before inserting the notification. A
  concurrent request with the same key blocks on the primary key until the first commits, then
  replays its notification; a test fires 10 at once and gets exactly one. Keys are scoped per
  user so two clients cannot collide, which matters once there is authentication and the
  scope becomes the caller's identity. The request body is hashed (SHA-256 of its fields in a
  fixed order): the same key with a different body is a client bug, answered with `422`.
  A replay returns the notification's current state rather than a stored copy of the first
  response, so it may say SENT where the first said QUEUED. Keys are never deleted yet; a
  24-hour expiry job belongs with the outbox cleanup.
- **Workers claim a notification before sending it.** A status check alone is not race-proof:
  during a rebalance two consumers can both read QUEUED. So the worker first inserts a row into
  `delivery_claims`, keyed by notification ID, with `INSERT ... ON CONFLICT DO UPDATE ... WHERE`
  the existing claim is older than a 60-second lease. Postgres serializes the inserts on the
  primary key, so exactly one worker wins; a test races 16 threads to check this. The lease uses
  the database clock, so clock skew between workers does not matter.
- **A held claim is retried, never skipped.** If another worker holds a live claim, the worker
  throws a retryable error instead of acknowledging the message. Otherwise a worker that crashed
  after claiming would lose the notification: the message would be acknowledged by the new
  owner and never seen again. The lease (60s) is shorter than the retry schedule (about 140s),
  so a later retry takes over a dead worker's claim. Send timeouts (SMTP 25s worst case, SMS 7s)
  stay under the lease so a slow send is not claimed twice. After winning the claim the worker checks the status again,
  in case the previous owner finished just before its lease expired. A transient send failure
  releases the claim so the retry need not wait for the lease.
- **Retries go to retry topics, not back to the front of the partition.** A transient failure
  publishes the record to `<topic>-retry-0`, then `-retry-1` and so on, five retries in all, and
  finally to `<topic>-dlt`. Retrying in place would keep per-user order, but one slow record would
  stall its whole partition: during an SMS provider outage, every user on that partition would
  wait. The cost is order: a retried notification can arrive after a newer one for the same
  user. That is fine for independent notifications, but not for, say, "order shipped"
  overtaking "order confirmed". Those would need a per-user sequence number the worker
  enforces. Malformed records skip the retries and go straight to the dead-letter topic.
- **Each record gets its own jittered delay.** Retry `i` waits `min(2s * 3^i, 60s)` plus or minus
  1s. Spring Kafka can add jitter, but it draws it once per topic at startup, so every record in
  a retry topic would still wait the same delay: when an outage fails a thousand records in the
  same second, they would all retry in the same second and could knock the provider over again.
  `RetryJitter` replaces the due-time header Spring writes with one drawn per record, and a test
  checks that twenty records failing together get spread-out delays.
- **Dead letters are marked FAILED, then can be replayed.** When the last retry fails, the
  dead-letter handler marks the notification FAILED with `RETRIES_EXHAUSTED`, so clients see a
  final status instead of QUEUED forever. Once the cause is fixed, the replay tool
  (`--spring.profiles.active=replay --topic=<topic>-dlt`) requeues each notification and
  publishes it to the main topic again. It commits each record's offset after publishing it, so
  a crash mid-replay at worst replays a record twice, which the claim absorbs. Only
  `RETRIES_EXHAUSTED` notifications are requeued; a rejected recipient stays FAILED.
- **SMS timeouts are not failures.** A timeout means the worker does not know whether the
  provider accepted the SMS. The worker retries with the same `Idempotency-Key` (the
  notification ID), and the provider accepts each key once. The mock provider has a fault for
  exactly this: it accepts the SMS, then answers after the worker has given up. In the chaos
  run, the key absorbed 53 repeat requests that would otherwise have texted users twice.
- **Temporary versus permanent email failures.** Only a refusal from the mail server, such as
  an unknown mailbox, marks a notification FAILED. Connection errors and timeouts are retried,
  because marking them FAILED would make the status final and block any later retry.
- **Preferences are applied at delivery time, by the worker.** Checking them when the API
  accepts a notification would be simpler, but a user who turns SMS off at 9pm expects the
  SMS still queued from 8:59pm not to arrive. The worker checks them only after winning the
  claim: if it checked first, one worker could mark a notification DEFERRED while another was
  already sending it, and the released copy would be sent a second time. A disabled channel
  ends as SUPPRESSED (`CHANNEL_DISABLED`), which is final; turning the channel back on does
  not resurrect old notifications.
- **Quiet hours defer, they do not drop.** During quiet hours the worker marks the
  notification DEFERRED with `deliver_after` set to the end of the window, computed in the
  user's time zone (an end time skipped by a daylight-saving change moves to just after the
  gap). Kafka cannot delay a single message for hours, so a scheduled job in the API, which owns
  the outbox, moves due notifications back to QUEUED and adds a new outbox row in one
  transaction. It uses `FOR UPDATE SKIP LOCKED`, like the outbox publisher, and the database
  clock. The worker checks the preferences again when the released message arrives, so a
  user who lengthens their quiet hours in the meantime is deferred again. Nothing yet lets an
  urgent notification, such as a password reset, skip quiet hours; that comes with priorities.
- **Rate limits are a token bucket in one Lua script.** Each user has a bucket per channel in
  Redis, shared by every worker. Refilling, checking and taking a token happen in one script,
  which Redis runs atomically, so two workers handling the same user at once cannot both take
  the last token; a test fires 50 requests at a bucket of 10 and gets exactly 10. The script
  reads the time from Redis, so workers with skewed clocks share one timeline, and a bucket
  key expires once it would be full again, so idle users cost no memory.
- **Over the limit means later, not never.** When the bucket is empty, the script lets the
  count go negative and returns how long until that caller's token exists: each caller
  reserves the next free slot. The notification is DEFERRED to exactly that slot and released
  by the same job as quiet hours, marked so that it does not take a second token. The simpler
  design, defer and try again, makes a burst of 100 retry together every cycle, and most fail
  again; reservations drain the burst at the refill rate with one deferral each. A slot more
  than an hour away is not reserved, and the notification is SUPPRESSED as `RATE_LIMITED`. A
  send that fails after taking a token keeps it for its retry. Quiet hours are checked first,
  so a notification held overnight does not use up the morning's tokens. In the chaos run,
  642 of 1,000 SMS waited for a slot, and all 1,000 were still sent exactly once.
- **If Redis is down, the limiter lets notifications through.** Failing closed would hold back
  password resets during a Redis outage; the limit protects users from floods, and a short
  outage lets at most a short flood through. The Redis client times out after 2s.
- **Public failure reasons are codes.** The API returns `RECIPIENT_REJECTED`,
  `REJECTED_BY_PROVIDER` or `RETRIES_EXHAUSTED`, never the raw provider error, which can reveal
  internal hostnames and ports. The full error goes to the logs. For SMS, a `400` is a rejected
  recipient, `408`, `429` and `5xx` are retried, and other `4xx` are rejected by the provider.
- **Crash between sending and recording.** If the worker dies after the email was sent but
  before marking it SENT, the retry sends the email again once the claim's lease expires. The
  claim cannot close this window, because SMTP has no idempotency key: the worker cannot ask the
  server whether a message already went out. For SMS the provider's key closes it.
- **When Postgres is down, records drain to the dead-letter topic.** Retry topics cannot tell a
  database outage from a provider outage, so after about 140s every record lands in the
  dead-letter topic, where marking it FAILED fails too, so the dead-letter handler keeps
  retrying. Replay recovers them afterwards. A blocking retry for database errors in front of the
  retry topics would avoid this; it is not built yet.
- **The outbox grows forever.** Published rows are never deleted. The partial index keeps polling
  fast, but a retention job should delete rows published more than a few days ago.

## Chaos test

`./mvnw -Pchaos verify` (also a CI job) starts the real jars: mock-sms with 10% failures, 5%
timeouts and 10% slow responses, the api, and three workers, against Postgres and Kafka in
containers. Eight clients send 1,000 SMS notifications at about 20 a second, retrying with the
same `Idempotency-Key` whenever the API is unreachable. Every 7 seconds one or two workers are
killed with the equivalent of `kill -9` and replaced; once, the API is killed instead. Each
of the 50 users gets 20 SMS against a limit of 5 a minute, so most notifications are also
deferred to a rate-limit slot and released later.

The test then checks that the API created exactly 1,000 notifications, that all 1,000 are SENT,
and that the provider accepted exactly one SMS for each of them. First run on a laptop:

| Measure | Value |
|---|---|
| Processes killed | 11 workers and the API |
| Faults injected by the provider | 121 failures, 48 timeouts, 128 slow responses |
| Notifications created / SENT / accepted by the provider | 1,000 / 1,000 / 1,000 |
| Repeat requests absorbed by the provider's idempotency key | 53 |
| Client retries while the API was down | 446 |

The 53 absorbed requests are the honest number: the system *did* send those SMS twice, and only
the provider-side key kept users from receiving them twice.

## Hardest bug

- _To be written._
