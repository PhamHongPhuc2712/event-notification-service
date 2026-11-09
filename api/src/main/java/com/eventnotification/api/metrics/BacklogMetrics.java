package com.eventnotification.api.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

/**
 * Backlog gauges, exported to Prometheus as {@code notification_outbox_backlog},
 * {@code notification_outbox_oldest_age_seconds} and {@code notification_deferred_backlog}.
 *
 * <p>The database is read on a schedule rather than on every scrape, so a slow or unreachable
 * database never slows the metrics endpoint. If a read fails, the gauges report NaN, which
 * Grafana shows as a gap, rather than a stale value that looks healthy.
 */
@Component
public class BacklogMetrics {

    public static final String OUTBOX_BACKLOG = "notification.outbox.backlog";
    public static final String OUTBOX_OLDEST_AGE = "notification.outbox.oldest.age";
    public static final String DEFERRED_BACKLOG = "notification.deferred.backlog";

    private static final Logger log = LoggerFactory.getLogger(BacklogMetrics.class);

    private record Snapshot(double outboxRows, double oldestOutboxAgeSeconds, double deferred) {

        static final Snapshot UNKNOWN = new Snapshot(Double.NaN, Double.NaN, Double.NaN);
    }

    private final BacklogRepository repository;
    private final AtomicReference<Snapshot> latest = new AtomicReference<>(Snapshot.UNKNOWN);

    public BacklogMetrics(BacklogRepository repository, MeterRegistry registry) {
        this.repository = repository;
        gauge(registry, OUTBOX_BACKLOG, "Outbox rows not yet published to Kafka", null, Snapshot::outboxRows);
        gauge(registry, OUTBOX_OLDEST_AGE, "How long the oldest unpublished outbox row has waited", "seconds",
                Snapshot::oldestOutboxAgeSeconds);
        gauge(registry, DEFERRED_BACKLOG, "Notifications held for quiet hours or a rate-limit slot", null,
                Snapshot::deferred);
    }

    @Scheduled(fixedDelayString = "${notifications.metrics.backlog-refresh-interval:5s}")
    public void refresh() {
        try {
            BacklogRepository.Backlog backlog = repository.read();
            latest.set(new Snapshot(backlog.outboxRows(), backlog.oldestOutboxAge().toMillis() / 1000.0,
                    backlog.deferred()));
        } catch (RuntimeException e) {
            latest.set(Snapshot.UNKNOWN);
            log.warn("Could not read the backlog for metrics: {}", e.getMessage());
        }
    }

    private void gauge(MeterRegistry registry, String name, String description, String baseUnit,
                       ToDoubleFunction<Snapshot> value) {
        Gauge.builder(name, latest, ref -> value.applyAsDouble(ref.get()))
                .description(description)
                .baseUnit(baseUnit)
                .strongReference(true)
                .register(registry);
    }
}
