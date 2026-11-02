package com.eventnotification.api.notification;

import java.time.Instant;
import java.util.UUID;

public record NotificationResponse(
        UUID id,
        String userId,
        Channel channel,
        String recipient,
        String subject,
        NotificationStatus status,
        String failureReason,
        Instant createdAt,
        Instant updatedAt,
        Instant sentAt) {

    static NotificationResponse from(Notification n) {
        return new NotificationResponse(n.id(), n.userId(), n.channel(), n.recipient(), n.subject(), n.status(),
                n.failureReason(), n.createdAt(), n.updatedAt(), n.sentAt());
    }
}
