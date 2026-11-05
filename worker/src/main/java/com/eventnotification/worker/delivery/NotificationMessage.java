package com.eventnotification.worker.delivery;

import java.time.Instant;
import java.util.UUID;

/**
 * The Kafka message contract published by the api module's outbox.
 *
 * @param priority NORMAL or HIGH; messages published before priorities existed have none
 */
public record NotificationMessage(
        UUID notificationId,
        String userId,
        String channel,
        String recipient,
        String subject,
        String body,
        String priority,
        Instant createdAt) {

    public static final String HIGH = "HIGH";

    /** A NORMAL-priority message. */
    public NotificationMessage(UUID notificationId, String userId, String channel, String recipient, String subject,
                               String body, Instant createdAt) {
        this(notificationId, userId, channel, recipient, subject, body, "NORMAL", createdAt);
    }

    public boolean isHighPriority() {
        return HIGH.equals(priority);
    }
}
