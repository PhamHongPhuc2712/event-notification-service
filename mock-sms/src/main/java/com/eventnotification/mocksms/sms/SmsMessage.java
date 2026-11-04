package com.eventnotification.mocksms.sms;

import java.time.Instant;
import java.util.UUID;

/** An SMS the provider accepted. Each idempotency key is accepted at most once. */
public record SmsMessage(UUID id, String idempotencyKey, String to, String body, Instant acceptedAt) {
}
