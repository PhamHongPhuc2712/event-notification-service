package com.eventnotification.api.outbox;

import com.eventnotification.api.config.NotificationProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Polls the outbox and publishes rows to Kafka.
 *
 * <p>Guarantee: at-least-once. Rows are marked published only after Kafka acknowledges every
 * send in the batch. If the process crashes between the ack and the commit, the batch is sent
 * again on the next poll, so consumers must be idempotent.
 *
 * <p>Counts published rows in {@code notification_outbox_published_total} and failed batches in
 * {@code notification_outbox_publish_failures_total}.
 */
@Component
public class OutboxPublisher {

    public static final String PUBLISHED = "notification.outbox.published";
    public static final String PUBLISH_FAILURES = "notification.outbox.publish.failures";

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final NotificationProperties.Outbox config;
    private final Counter published;
    private final Counter publishFailures;
    private final Clock clock;

    public OutboxPublisher(OutboxRepository outbox, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                           NotificationProperties properties, MeterRegistry registry, Clock clock) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.tx = tx;
        this.config = properties.outbox();
        this.published = Counter.builder(PUBLISHED)
                .description("Outbox rows published to Kafka")
                .register(registry);
        this.publishFailures = Counter.builder(PUBLISH_FAILURES)
                .description("Outbox batches that failed to publish and will be retried")
                .register(registry);
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${notifications.outbox.poll-interval}")
    void poll() {
        try {
            publishBatch();
        } catch (OutboxPublishException e) {
            publishFailures.increment();
            log.warn("Outbox batch not published, will retry on next poll: {}", e.getMessage(), e.getCause());
        }
    }

    /** Publishes one batch and returns how many rows were published. */
    public int publishBatch() {
        Integer count = tx.execute(status -> {
            List<OutboxEvent> batch = outbox.lockUnpublished(config.batchSize());
            if (batch.isEmpty()) {
                return 0;
            }
            sendAll(batch);
            outbox.markPublished(batch.stream().map(OutboxEvent::id).toList(), clock.instant());
            return batch.size();
        });
        int rows = count == null ? 0 : count;
        published.increment(rows);
        return rows;
    }

    private void sendAll(List<OutboxEvent> batch) {
        CompletableFuture<?>[] sends = batch.stream()
                .map(e -> kafka.send(e.topic(), e.messageKey(), e.payload()))
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(sends).get(config.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException("Interrupted while publishing " + batch.size() + " outbox rows", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new OutboxPublishException("Failed to publish " + batch.size() + " outbox rows starting at id "
                    + batch.getFirst().id(), e);
        }
    }
}
