// Load test: POST /notifications at a constant rate.
//
// Every request carries a unique Idempotency-Key, and a request that fails with a network error
// or a 5xx is retried with the same key, as a well-behaved client would. That way a run that
// restarts the API still counts each notification exactly once, and the verifier can check
// that every accepted notification was delivered exactly once.
//
// Run through loadtest/run.sh, or directly:
//   docker run --rm -i --network host -e RUN_ID=demo grafana/k6:1.4.2 run - < loadtest/notifications.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RUN_ID = __ENV.RUN_ID || `run${Date.now()}`;
const RATE = Number(__ENV.RATE || 500);                 // requests per second
const DURATION = __ENV.DURATION || '60s';
const USERS = Number(__ENV.USERS || 50000);             // spread over many users, as in production
const SMS_SHARE = Number(__ENV.SMS_SHARE || 0.5);       // the rest is email
const HIGH_SHARE = Number(__ENV.HIGH_SHARE || 0.1);     // HIGH priority, such as login codes
const MAX_ATTEMPTS = Number(__ENV.MAX_ATTEMPTS || 60);  // enough to ride out an API restart

const accepted = new Counter('notifications_accepted');
const rejected = new Counter('notifications_rejected');
const gaveUp = new Counter('notifications_gave_up');
const clientRetries = new Counter('client_retries');
const acceptLatency = new Trend('accept_latency', true);

export const options = {
    scenarios: {
        notifications: {
            executor: 'constant-arrival-rate',
            rate: RATE,
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs: Math.max(50, Math.ceil(RATE / 4)),
            maxVUs: Math.max(200, RATE * 2),
        },
    },
    // Fail the run if requests were lost at the API.
    thresholds: {
        notifications_gave_up: ['count==0'],
        notifications_rejected: ['count==0'],
    },
    summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

export default function () {
    const key = `${RUN_ID}-${__VU}-${__ITER}`;
    const userId = `lt-${RUN_ID}-${Math.floor(Math.random() * USERS)}`;
    const body = JSON.stringify(notification(key, userId));
    const params = {
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
        timeout: '10s',
        tags: { name: 'POST /notifications' },
    };

    const started = Date.now();
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        const res = http.post(`${BASE_URL}/notifications`, body, params);
        if (res.status === 202) {
            accepted.add(1);
            acceptLatency.add(Date.now() - started);
            return;
        }
        if (res.status !== 0 && res.status < 500) {
            rejected.add(1);
            check(res, { 'accepted (202)': (r) => r.status === 202 });
            console.error(`${key} rejected with ${res.status}: ${res.body}`);
            return;
        }
        // Network error or 5xx: the API may be restarting. Same key, so no duplicate is created.
        clientRetries.add(1);
        sleep(Math.min(0.25 * attempt, 2));
    }
    gaveUp.add(1);
    console.error(`${key} gave up after ${MAX_ATTEMPTS} attempts`);
}

function notification(key, userId) {
    const roll = Math.random();
    const priority = Math.random() < HIGH_SHARE ? 'HIGH' : 'NORMAL';
    if (roll < SMS_SHARE) {
        // A valid E.164 number; the key in the body lets the verifier match the provider's copy.
        const number = `+65${String(80000000 + Math.floor(Math.random() * 19999999))}`;
        return { userId, channel: 'SMS', recipient: number, body: `Load test ${key}`, priority };
    }
    return {
        userId,
        channel: 'EMAIL',
        recipient: `${userId}@example.com`,
        subject: `Load test ${key}`,
        body: 'Sent by the k6 load test.',
        priority,
    };
}

export function handleSummary(data) {
    const metric = (name, stat) => (data.metrics[name] ? data.metrics[name].values[stat] : 0);
    const summary = {
        runId: RUN_ID,
        targetRate: RATE,
        duration: DURATION,
        accepted: metric('notifications_accepted', 'count'),
        rejected: metric('notifications_rejected', 'count'),
        gaveUp: metric('notifications_gave_up', 'count'),
        clientRetries: metric('client_retries', 'count'),
        requestsPerSecond: metric('http_reqs', 'rate'),
        acceptLatencyMs: {
            p50: metric('accept_latency', 'med'),
            p95: metric('accept_latency', 'p(95)'),
            p99: metric('accept_latency', 'p(99)'),
            max: metric('accept_latency', 'max'),
        },
        droppedIterations: metric('dropped_iterations', 'count'),
    };
    return {
        stdout: `\n${JSON.stringify(summary, null, 2)}\n`,
        [`/results/${RUN_ID}/k6-summary.json`]: JSON.stringify(summary, null, 2),
    };
}
