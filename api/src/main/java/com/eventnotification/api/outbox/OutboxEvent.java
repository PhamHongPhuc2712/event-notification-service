package com.eventnotification.api.outbox;

import java.util.UUID;

public record OutboxEvent(long id, UUID aggregateId, String topic, String messageKey, String payload) {
}
