package com.eventnotification.worker.logging;

import org.slf4j.MDC;

import java.util.UUID;

/**
 * The log context (MDC) key that ties log lines to a notification. The JSON log format writes
 * every MDC entry as a field, so each line about a notification can be found by its ID.
 */
public final class NotificationLogContext {

    public static final String NOTIFICATION_ID = "notificationId";

    private NotificationLogContext() {
    }

    /** Sets the notification ID until the returned handle is closed. */
    public static MDC.MDCCloseable put(UUID notificationId) {
        return MDC.putCloseable(NOTIFICATION_ID, notificationId.toString());
    }
}
