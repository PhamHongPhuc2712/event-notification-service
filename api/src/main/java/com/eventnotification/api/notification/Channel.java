package com.eventnotification.api.notification;

/** Delivery channels. */
public enum Channel {
    EMAIL,
    SMS,
    /** An HTTP POST, signed with HMAC-SHA256, to the recipient URL. */
    WEBHOOK
}
