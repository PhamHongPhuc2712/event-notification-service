# Results

Measured on one laptop: 12 vCPUs and 23 GB of RAM under WSL2, with Kafka, Postgres, Redis,
Mailpit and the monitoring stack in Docker, and the api, one worker, the mock SMS provider and
the k6 load generator on the same machine. Every number below comes from
[`loadtest/run.sh`](../loadtest/run.sh): k6 sends notifications at a constant rate for 90 seconds
(half SMS, half email, 10% HIGH priority, spread over 50,000 users), then
[`verify.py`](../loadtest/verify.py) waits for the backlog to drain and checks every accepted
notification against Postgres, the SMS provider and the Mailpit inbox.

## Summary

| | Before tuning | After |
|---|---|---|
| Sustained throughput (delivered, with lag staying bounded) | about 260 a second | **about 430 a second** |
| End-to-end latency at 200 a second, p50 / p95 / p99 | — | **156 / 244 / 1,086 ms** |
| End-to-end latency at 500 a second offered, p50 / p99 | 16.5 s / 48.9 s | 0.37 s / 8.9 s |
| Peak consumer lag at 500 a second offered | 5,140 | 1,923 |
| API response time at 500 a second, p95 | 3.4 s | 0.42 s |
| Notifications lost or duplicated, across 1,157,379 in 31 verified runs | 0 | **0** |

The target from the plan was 2,000 a second with a p99 under 200 ms. This laptop does not get
there, and the reason is measured below: every notification needs synchronous Postgres commits,
and the WSL2 virtual disk flushes only about 400 times a second.

## Finding and fixing the bottlenecks

Each step was measured at 500 notifications a second offered for 60–90 seconds.

| Step | Change | Delivered a second | End-to-end p50 / p99 | Peak lag | API p95 |
|---|---|---|---|---|---|
| 0 | Baseline | 264 | 16.5 s / 48.9 s | 5,140 | 3,366 ms |
| 1 | Bigger connection pools; one consumer thread per partition | 372 | 7.9 s / 17.9 s | 1,163 | 828 ms |
| 2 | Outbox publishes back to back while batches are full | 376 | 4.2 s / 16.2 s | 4,538 | 672 ms |
| 3 | Claim reads the state in the same statement; claim commits asynchronously | 431 | 0.37 s / 8.9 s | 1,923 | 421 ms |

**Step 1: threads waiting for connections and partitions without consumers.** The dashboard
and `hikaricp_connections_pending` showed up to 190 API requests queued for one of the 10
default database connections, which is where the 3.4-second API p95 came from. The worker ran 3
consumer threads for 6 partitions per topic. Raising the API pool to 30, the worker pool to 40
and the concurrency to 6 lifted delivery from 264 to 372 a second.

**Step 2: the outbox capped at 500 rows a second.** The new
`notification_outbox_backlog` and `notification_outbox_oldest_age_seconds` gauges showed the
outbox at 5,811 rows, the oldest 17 seconds old: one batch of 100 per 200 ms poll is at most 500
rows a second, and less once publishing takes time. The publisher now keeps publishing while
batches come back full. The outbox stayed empty afterwards, and the backlog moved to Kafka,
where the workers were now the limit.

**Step 3: workers waiting for Postgres.** Thread dumps of the worker's consumer threads under
load (`jstack`, 180 samples) put 63% of their time in two statements, claiming the notification
and marking it SENT, both waiting for the server. Postgres itself was mostly idle except for
`IO:WALSync` and `LWLock:WALWrite` waits: commits waiting for the WAL to reach disk.
`pg_test_fsync` on the data volume measured `fdatasync` at **396 a second (2.5 ms each)**. Every
notification needs three synchronous commits (the API's insert, the worker's claim and marking
it SENT), and they all share that flush rate.

Two changes followed. The claim now reads the notification's status and rate-limit reservation
in the same statement, so a delivery makes one round trip before the send instead of four. And
the claim commits asynchronously, which is safe because the later synchronous SENT commit
flushes it; the reasoning is in [DESIGN.md](DESIGN.md). Marking SENT and the API's insert stay
synchronous.

**Confirming the disk is the limit.** With `synchronous_commit` turned off for the whole
database, as an experiment only, the API took 911 requests a second with a p99 of 98 ms, and the
workers delivered 525 a second. Past that, the workers are limited by parallelism: 6 partitions
per topic means at most 12 busy consumer threads.

Two things that did not help, kept out of the code: Postgres group commit (`commit_delay`),
which gave the same 426 a second in two repeat runs, and tuning that only moves load between
the API and the workers.

## Throughput and latency after tuning

The project's default configuration, one worker, 90 seconds per run.

| Offered | Accepted a second | Delivered a second | End-to-end p50 / p95 / p99 | Peak lag | API p50 / p95 / p99 | Lost | Duplicated |
|---|---|---|---|---|---|---|---|
| 200/s | 181 | 181 | 156 / 244 / 1,086 ms | 40 | 5 / 12 / 512 ms | 0 | 0 |
| 400/s | 373 | 371 | 222 / 1,628 / 3,108 ms | 719 | 9 / 274 / 804 ms | 0 | 0 |
| 500/s | 452 | 431 | 365 / 6,662 / 8,875 ms | 1,923 | 11 / 421 / 996 ms | 0 | 0 |
| 600/s | 532 | 419 | 5,780 / 37,407 / 41,806 ms | 12,365 | 15 / 1,346 / 4,470 ms | 0 | 0 |

Up to about 400 a second the workers keep up and the backlog stays small; at 500 a second they
fall a little behind and catch up after the burst; at 600 a second the backlog grows for as long
as the load lasts. In every run, every accepted notification was delivered exactly once.

"Accepted a second" is below the offered rate because k6 skips a request when all its virtual
users are busy waiting for slow responses; the skipped requests were never sent, so they are not
counted as accepted.

## What it would take to go faster

- **A disk with faster flushes.** A server NVMe drive flushes tens of thousands of times a second;
  the remaining synchronous commits would stop being the limit.
- **More partitions and workers.** Throughput past the database limit scales with consumer
  threads, which are capped by partitions per topic (6 now).
- **Fewer synchronous commits per notification.** Marking several notifications SENT in one
  commit would need batch listeners, which do not fit Spring Kafka's per-record retry topics.
