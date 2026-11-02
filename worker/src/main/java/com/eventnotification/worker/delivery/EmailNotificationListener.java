package com.eventnotification.worker.delivery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
class EmailNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationListener.class);

    private final EmailDeliveryService delivery;
    private final JsonMapper jsonMapper;

    EmailNotificationListener(EmailDeliveryService delivery, JsonMapper jsonMapper) {
        this.delivery = delivery;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(
            id = "email-worker",
            topics = "${notifications.topics.email}",
            groupId = "${notifications.consumer.email-group}",
            concurrency = "${notifications.consumer.concurrency}")
    void onMessage(ConsumerRecord<String, String> record) {
        NotificationMessage message = parse(record);
        if (message == null || message.notificationId() == null) {
            // A malformed message will never parse, so retrying cannot help. Weeks 3-4 send it
            // to a dead-letter topic instead of only logging it.
            log.error("Skipping malformed message at {}-{}@{}", record.topic(), record.partition(), record.offset());
            return;
        }
        delivery.deliver(message);
    }

    /** Returns null for empty values, the JSON literal null, and invalid JSON. */
    private NotificationMessage parse(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            return null;
        }
        try {
            return jsonMapper.readValue(record.value(), NotificationMessage.class);
        } catch (JacksonException e) {
            log.debug("Invalid JSON at {}-{}@{}: {}", record.topic(), record.partition(), record.offset(),
                    e.getOriginalMessage());
            return null;
        }
    }
}
