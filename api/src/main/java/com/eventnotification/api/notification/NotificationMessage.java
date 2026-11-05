package com.eventnotification.api.notification;

import java.time.Instant;
import java.util.UUID;

/** The Kafka message contract. The worker module has a matching record. */
public record NotificationMessage(
        UUID notificationId,
        String userId,
        Channel channel,
        String recipient,
        String subject,
        String body,
        Priority priority,
        Instant createdAt) {

    static NotificationMessage from(Notification n) {
        return new NotificationMessage(n.id(), n.userId(), n.channel(), n.recipient(), n.subject(), n.body(),
                n.priority(), n.createdAt());
    }
}
