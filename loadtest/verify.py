"""Checks a load-test run: was every accepted notification delivered, exactly once?

Compares three independent records of the run:
  * what k6 was told: the number of requests the API accepted (k6-summary.json),
  * what the service recorded: the run's rows in Postgres and their statuses,
  * what the providers received: SMS at the mock provider (one entry per Idempotency-Key, plus a
    count of repeated requests it absorbed) and emails in Mailpit (every copy, so a duplicate
    email shows up as two messages with the same subject).

A notification is LOST if it was accepted but never reached its provider, and DUPLICATED if the
user would have received it more than once. Exits with status 1 if anything was lost or
duplicated, so a script or CI job can fail on it.

Usage:
    python loadtest/verify.py --run-id <id> [--timeout 600]
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
PENDING = ("QUEUED", "DEFERRED")


@dataclass
class Report:
    run_id: str
    accepted_by_api: int = 0
    created: int = 0
    statuses: dict[str, int] = field(default_factory=dict)
    pending_after_timeout: int = 0
    sms_expected: int = 0
    sms_delivered: int = 0
    sms_lost: list[str] = field(default_factory=list)
    sms_repeats_absorbed: int = 0
    email_expected: int = 0
    email_delivered: int = 0
    email_lost: list[str] = field(default_factory=list)
    email_duplicates: list[str] = field(default_factory=list)
    lost_at_api: int = 0
    first_created: str | None = None
    last_sent: str | None = None
    delivered_per_second: float = 0.0
    latency_ms: dict[str, float] = field(default_factory=dict)
    max_consumer_lag: float | None = None

    @property
    def lost(self) -> int:
        return self.lost_at_api + len(self.sms_lost) + len(self.email_lost)

    @property
    def duplicated(self) -> int:
        return len(self.email_duplicates)


def main() -> int:
    args = parse_args()
    summary = json.loads((ROOT / "loadtest" / "results" / args.run_id / "k6-summary.json").read_text())
    report = Report(run_id=args.run_id, accepted_by_api=int(summary["accepted"]))

    with psycopg.connect(dsn(), autocommit=True) as db:
        wait_for_drain(db, args.run_id, args.timeout)
        read_database(db, report)
        sms_rows = rows(db, args.run_id, "SMS")
        email_rows = rows(db, args.run_id, "EMAIL")

    check_sms(report, sms_rows, args.mock_sms)
    check_email(report, email_rows, args.mailpit, args.run_id)
    report.lost_at_api = max(0, report.accepted_by_api - report.created)
    report.max_consumer_lag = max_consumer_lag(args.prometheus, report)

    out = ROOT / "loadtest" / "results" / args.run_id / "verify.json"
    payload = {**report.__dict__, "lost": report.lost, "duplicated": report.duplicated}
    out.write_text(json.dumps(payload, indent=2, default=str))
    print(render(report, summary))
    return 1 if report.lost or report.duplicated or report.pending_after_timeout else 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", required=True)
    p.add_argument("--timeout", type=int, default=600, help="seconds to wait for the run to drain")
    p.add_argument("--mock-sms", default="http://localhost:8082")
    p.add_argument("--mailpit", default="http://localhost:8025")
    p.add_argument("--prometheus", default="http://localhost:9090")
    return p.parse_args()


def dsn() -> str:
    """Connection settings from infra/.env, the same file the services read."""
    env = {}
    for line in (ROOT / "infra" / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return psycopg.conninfo.make_conninfo(
        host="localhost", port=5432, dbname=env.get("POSTGRES_DB", "notifications"),
        user=env.get("POSTGRES_USER", "notifications"), password=env["POSTGRES_PASSWORD"])


def run_filter(run_id: str) -> str:
    return f"lt-{run_id}-%"


def wait_for_drain(db: psycopg.Connection, run_id: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while True:
        pending = db.execute("SELECT count(*) FROM notifications WHERE user_id LIKE %s AND status = ANY(%s)",
                             (run_filter(run_id), list(PENDING))).fetchone()[0]
        if pending == 0 or time.monotonic() > deadline:
            return
        if pending != last:
            print(f"waiting: {pending} notifications still queued or deferred", file=sys.stderr)
            last = pending
        time.sleep(2)


def read_database(db: psycopg.Connection, report: Report) -> None:
    pattern = run_filter(report.run_id)
    report.statuses = dict(db.execute(
        "SELECT status, count(*) FROM notifications WHERE user_id LIKE %s GROUP BY status ORDER BY status",
        (pattern,)).fetchall())
    report.created = sum(report.statuses.values())
    report.pending_after_timeout = sum(report.statuses.get(s, 0) for s in PENDING)
    first, last, sent = db.execute(
        "SELECT min(created_at), max(sent_at), count(sent_at) FROM notifications WHERE user_id LIKE %s",
        (pattern,)).fetchone()
    report.first_created, report.last_sent = (first.isoformat() if first else None,
                                              last.isoformat() if last else None)
    if first and last and last > first:
        report.delivered_per_second = round(sent / (last - first).total_seconds(), 1)
    percentiles, mx = db.execute(
        """SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY ms), max(ms)
           FROM (SELECT EXTRACT(EPOCH FROM sent_at - created_at) * 1000 AS ms
                 FROM notifications WHERE user_id LIKE %s AND sent_at IS NOT NULL) t""",
        (pattern,)).fetchone()
    if percentiles:
        p50, p95, p99 = percentiles
        report.latency_ms = {"p50": round(p50, 1), "p95": round(p95, 1), "p99": round(p99, 1),
                             "max": round(float(mx), 1)}


def rows(db: psycopg.Connection, run_id: str, channel: str) -> list[tuple]:
    return db.execute("SELECT id::text, status, subject FROM notifications WHERE user_id LIKE %s AND channel = %s",
                      (run_filter(run_id), channel)).fetchall()


def check_sms(report: Report, sms_rows: list[tuple], base_url: str) -> None:
    """The provider keeps one entry per Idempotency-Key, which the worker sets to the notification ID."""
    delivered = {m["idempotencyKey"] for m in get_json(f"{base_url}/v1/messages")}
    expected = {nid for nid, status, _ in sms_rows if status not in ("FAILED", "SUPPRESSED")}
    report.sms_expected = len(expected)
    report.sms_delivered = len(expected & delivered)
    report.sms_lost = sorted(expected - delivered)
    report.sms_repeats_absorbed = int(get_json(f"{base_url}/v1/messages/stats")["duplicateRequests"])


def check_email(report: Report, email_rows: list[tuple], base_url: str, run_id: str) -> None:
    """Mailpit keeps every copy, so the same subject twice means the user got the email twice."""
    subjects = collections.Counter(mailpit_subjects(base_url, run_id))
    expected = {subject for _, status, subject in email_rows if status not in ("FAILED", "SUPPRESSED")}
    report.email_expected = len(expected)
    report.email_delivered = len(expected & subjects.keys())
    report.email_lost = sorted(expected - subjects.keys())
    report.email_duplicates = sorted(s for s, n in subjects.items() if n > 1)


def mailpit_subjects(base_url: str, run_id: str) -> list[str]:
    query = urllib.parse.quote(f'subject:"Load test {run_id}-"')
    subjects, start = [], 0
    while True:
        page = get_json(f"{base_url}/api/v1/search?query={query}&start={start}&limit=1000")
        messages = page.get("messages") or []
        subjects += [m["Subject"] for m in messages]
        start += len(messages)
        if not messages or start >= page.get("messages_count", 0):
            return subjects


def max_consumer_lag(prometheus: str, report: Report) -> float | None:
    """Highest total consumer lag while the run was in flight, if Prometheus is running."""
    if not report.first_created or not report.last_sent:
        return None
    from datetime import datetime
    start = datetime.fromisoformat(report.first_created).timestamp()
    end = datetime.fromisoformat(report.last_sent).timestamp() + 10
    query = ('max_over_time(sum(clamp_min(kafka_consumergroup_lag{consumergroup=~"(email|sms|webhook)-worker.*"}, 0))'
             f'[{max(int(end - start), 10)}s:5s])')
    try:
        result = get_json(f"{prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": end}))
        values = result["data"]["result"]
        return float(values[0]["value"][1]) if values else None
    except OSError:
        return None


def get_json(url: str):
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.load(response)


def render(r: Report, k6: dict) -> str:
    verdict = "PASS" if not (r.lost or r.duplicated or r.pending_after_timeout) else "FAIL"
    lines = [
        f"## Run {r.run_id}: {verdict}",
        "",
        "| Measure | Value |",
        "|---|---|",
        f"| Target rate | {k6['targetRate']}/s for {k6['duration']} |",
        f"| API requests per second achieved | {k6['requestsPerSecond']:.0f} |",
        f"| API accept latency p50 / p95 / p99 | {k6['acceptLatencyMs']['p50']:.0f} / "
        f"{k6['acceptLatencyMs']['p95']:.0f} / {k6['acceptLatencyMs']['p99']:.0f} ms |",
        f"| Accepted by the API (k6) | {r.accepted_by_api} |",
        f"| Stored by the API | {r.created} |",
        f"| Final statuses | {r.statuses} |",
        f"| Delivered per second (first accept to last send) | {r.delivered_per_second} |",
        f"| End-to-end latency p50 / p95 / p99 / max | {r.latency_ms.get('p50')} / {r.latency_ms.get('p95')} / "
        f"{r.latency_ms.get('p99')} / {r.latency_ms.get('max')} ms |",
        f"| Peak consumer lag | {r.max_consumer_lag if r.max_consumer_lag is not None else 'n/a'} |",
        f"| SMS delivered / expected | {r.sms_delivered} / {r.sms_expected} |",
        f"| Emails delivered / expected | {r.email_delivered} / {r.email_expected} |",
        f"| **Lost** | **{r.lost}** |",
        f"| **Duplicated** (user received twice) | **{r.duplicated}** |",
        f"| Repeat SMS requests absorbed by the provider's key | {r.sms_repeats_absorbed} |",
        f"| Client retries (API unreachable or 5xx) | {k6['clientRetries']} |",
        f"| Requests k6 could not start in time | {k6['droppedIterations']} |",
    ]
    if r.pending_after_timeout:
        lines.append(f"\n{r.pending_after_timeout} notifications were still pending when the verifier gave up.")
    for name, items in (("Lost SMS", r.sms_lost), ("Lost emails", r.email_lost),
                        ("Duplicated emails", r.email_duplicates)):
        if items:
            lines.append(f"\n{name} ({len(items)}): {', '.join(items[:10])}{' ...' if len(items) > 10 else ''}")
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
