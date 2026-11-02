package com.eventnotification.worker.delivery;

import java.time.Instant;
import java.util.UUID;

/** The Kafka message contract published by the api module's outbox. */
public record NotificationMessage(
        UUID notificationId,
        String userId,
        String channel,
        String recipient,
        String subject,
        String body,
        Instant createdAt) {
}
