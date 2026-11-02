package com.eventnotification.worker.delivery;

public enum DeliveryOutcome {
    SENT,
    FAILED,
    SKIPPED_ALREADY_PROCESSED,
    SKIPPED_UNKNOWN
}
