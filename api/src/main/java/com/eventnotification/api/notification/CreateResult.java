package com.eventnotification.api.notification;

/**
 * @param replayed true if an earlier request with the same idempotency key created the notification
 */
public record CreateResult(Notification notification, boolean replayed) {
}
