# Load tests

A k6 script that sends notifications at a constant rate, and a verifier that checks every
accepted notification was delivered exactly once. Results of the runs are in
[docs/RESULTS.md](../docs/RESULTS.md).

| File | What it does |
|---|---|
| `notifications.js` | k6 scenario: POST /notifications at `RATE` per second, half SMS and half email, 10% HIGH priority, spread over 50,000 users. Each request has a unique `Idempotency-Key` and is retried with it on a network error or 5xx. |
| `verify.py` | Waits for the run to drain, then compares what k6 was told (accepted) with Postgres (stored, statuses, latency) and with the providers (SMS at the mock provider, emails in Mailpit). Exits 1 if anything was lost or duplicated. |
| `run.sh` | Resets the mock provider and Mailpit, runs k6 in Docker, then runs the verifier. |
| `chaos.sh` | A load test while workers, Kafka and the API are killed and the network to Kafka and Postgres slows down, through Toxiproxy. Starts and stops its own services. |

## Running one

Prerequisites: the infrastructure with the observability profile, the three services, and the
Python environment from the main README (`pip install -r requirements.txt`).

```bash
RATE=1000 DURATION=60s bash loadtest/run.sh
```

Watch it live on the Grafana dashboard. Each run writes `k6-summary.json`, `verify.json` and a
`report.md` table to `loadtest/results/<run id>/` (gitignored).

By default the mock SMS provider's faults are turned off, so the run measures the service rather
than the fake provider; `SMS_FAULTS=1` keeps them, and a JSON object, as `PUT /admin/faults`
takes it, sets them. `K6_HOOK` runs a command alongside k6.

## How lost and duplicated are decided

- **Lost:** accepted by the API but missing from Postgres, or never received by its provider.
- **Duplicated:** the user would have received it twice. Mailpit keeps every copy, so the same
  subject twice is a duplicate email. The SMS provider accepts each `Idempotency-Key` once and
  counts the repeats it absorbed; those are reported separately, because the user got one SMS.

## Chaos run

```bash
./mvnw -q package -DskipTests
docker compose -f infra/docker-compose.yml --profile observability --profile chaos up -d
RATE=300 DURATION=150s bash loadtest/chaos.sh
```

It needs ports 8080–8082 and 8091 free, since it starts its own api, two workers and the mock
provider, connected through Toxiproxy. The timeline of what it broke is in
`loadtest/results/<run id>/chaos.log`, next to every service's log.

