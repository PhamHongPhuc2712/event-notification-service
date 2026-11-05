package com.eventnotification.api.notification;

import java.time.Instant;
import java.util.UUID;

public record Notification(
        UUID id,
        String userId,
        Channel channel,
        String recipient,
        String subject,
        String body,
        Priority priority,
        NotificationStatus status,
        String failureReason,
        Instant createdAt,
        Instant updatedAt,
        Instant sentAt,
        /** When a DEFERRED notification is released; null otherwise. */
        Instant deliverAfter) {

    public static Notification queued(CreateNotificationRequest request, Instant now) {
        return new Notification(UUID.randomUUID(), request.userId(), request.channel(), request.recipient(),
                request.subject(), request.body(), request.effectivePriority(), NotificationStatus.QUEUED, null, now, now, null, null);
    }
}
