package com.eventnotification.api.outbox;

import com.eventnotification.api.config.NotificationProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

/**
 * Deletes outbox rows that were published to Kafka more than the retention period ago.
 *
 * <p>Without it the outbox only grows: after about 1.3 million notifications its indexes no
 * longer fit in Postgres's memory, and the load test delivered 157 notifications a second instead
 * of 395 on the same setup with empty tables. A published row is not needed again: the
 * notification itself keeps its status, and the message is already in Kafka.
 *
 * <p>Counts deleted rows in {@code notification_outbox_deleted_total} and failed runs in
 * {@code notification_outbox_cleanup_failures_total}.
 */
@Component
public class OutboxCleaner {

    public static final String DELETED = "notification.outbox.deleted";
    public static final String FAILURES = "notification.outbox.cleanup.failures";

    private static final Logger log = LoggerFactory.getLogger(OutboxCleaner.class);

    private final OutboxRepository outbox;
    private final NotificationProperties.Outbox.Cleanup config;
    private final Counter deleted;
    private final Counter failures;
    private final Clock clock;

    @Autowired
    public OutboxCleaner(OutboxRepository outbox, NotificationProperties properties, MeterRegistry registry,
                         Clock clock) {
        this(outbox, properties.outbox().cleanup(), registry, clock);
    }

    OutboxCleaner(OutboxRepository outbox, NotificationProperties.Outbox.Cleanup config, MeterRegistry registry,
                  Clock clock) {
        this.outbox = outbox;
        this.config = config;
        this.deleted = Counter.builder(DELETED)
                .description("Published outbox rows deleted after the retention period")
                .register(registry);
        this.failures = Counter.builder(FAILURES)
                .description("Outbox cleanup runs that failed and will run again at the next interval")
                .register(registry);
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${notifications.outbox.cleanup.interval}",
            initialDelayString = "${notifications.outbox.cleanup.interval}")
    void cleanUp() {
        try {
            deletePublished();
        } catch (DataAccessException e) {
            failures.increment();
            log.warn("Outbox cleanup failed, will run again in {}: {}", config.interval(), e.getMessage());
        }
    }

    /** Deletes batch after batch until the old published rows are gone; returns how many. */
    public int deletePublished() {
        Instant cutoff = clock.instant().minus(config.retention());
        int total = 0;
        int batch;
        do {
            batch = outbox.deletePublishedBefore(cutoff, config.batchSize());
            deleted.increment(batch);
            total += batch;
        } while (batch == config.batchSize());
        if (total > 0) {
            log.info("Deleted {} outbox rows published before {}", total, cutoff);
        }
        return total;
    }
}
