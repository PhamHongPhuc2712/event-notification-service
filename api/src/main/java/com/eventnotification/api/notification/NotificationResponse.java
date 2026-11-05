package com.eventnotification.api.notification;

import java.time.Instant;
import java.util.UUID;

public record NotificationResponse(
        UUID id,
        String userId,
        Channel channel,
        String recipient,
        String subject,
        Priority priority,
        NotificationStatus status,
        String failureReason,
        Instant createdAt,
        Instant updatedAt,
        Instant sentAt,
        Instant deliverAfter) {

    static NotificationResponse from(Notification n) {
        return new NotificationResponse(n.id(), n.userId(), n.channel(), n.recipient(), n.subject(), n.priority(), n.status(),
                n.failureReason(), n.createdAt(), n.updatedAt(), n.sentAt(), n.deliverAfter());
    }
}
