package com.eventnotification.worker;

import com.eventnotification.worker.config.WorkerProperties;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/** Worker settings for unit tests that build beans by hand. */
public final class TestWorkerProperties {

    public static final Duration CLAIM_LEASE = Duration.ofSeconds(60);

    private TestWorkerProperties() {
    }

    public static WorkerProperties create() {
        return new WorkerProperties(
                new WorkerProperties.Topics("notifications.email", "notifications.sms", 6),
                new WorkerProperties.Consumer("email-worker", "sms-worker", 1),
                new WorkerProperties.Mail("no-reply@example.com"),
                new WorkerProperties.Sms(URI.create("http://localhost:8082"), Duration.ofSeconds(1),
                        Duration.ofSeconds(1)),
                new WorkerProperties.Delivery(CLAIM_LEASE),
                new WorkerProperties.Retry(4, Duration.ofSeconds(2), 3, Duration.ofSeconds(60), Duration.ofSeconds(1)),
                new WorkerProperties.RateLimit(true, Duration.ofHours(1),
                        Map.of("SMS", new WorkerProperties.Bucket(5, Duration.ofSeconds(12)))));
    }
}
