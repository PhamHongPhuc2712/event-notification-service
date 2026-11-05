package com.eventnotification.api.notification;

public enum NotificationStatus {
    QUEUED,
    /** Held back by the user's quiet hours until {@code deliverAfter}. */
    DEFERRED,
    SENT,
    FAILED,
    /** Not sent because the user turned the channel off. */
    SUPPRESSED
}
