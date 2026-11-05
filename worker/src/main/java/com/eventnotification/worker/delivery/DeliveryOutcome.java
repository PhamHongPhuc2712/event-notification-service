package com.eventnotification.worker.delivery;

public enum DeliveryOutcome {
    SENT,
    FAILED,
    /** The user turned the channel off. */
    SUPPRESSED,
    /** It is the user's quiet hours; the notification is released when they end. */
    DEFERRED,
    SKIPPED_ALREADY_PROCESSED,
    SKIPPED_UNKNOWN
}
