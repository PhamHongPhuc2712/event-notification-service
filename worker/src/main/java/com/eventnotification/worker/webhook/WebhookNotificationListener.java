package com.eventnotification.worker.webhook;

import com.eventnotification.worker.delivery.DeliveryService;
import com.eventnotification.worker.delivery.MessageParser;
import com.eventnotification.worker.retry.DeadLetterHandler;
import com.eventnotification.worker.retry.NotificationRetryableTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class WebhookNotificationListener {

    private final DeliveryService delivery;
    private final WebhookSender sender;
    private final MessageParser parser;
    private final DeadLetterHandler deadLetters;

    WebhookNotificationListener(DeliveryService delivery, WebhookSender sender, MessageParser parser,
                                DeadLetterHandler deadLetters) {
        this.delivery = delivery;
        this.sender = sender;
        this.parser = parser;
        this.deadLetters = deadLetters;
    }

    @NotificationRetryableTopic
    @KafkaListener(
            id = "webhook-worker",
            topics = "${notifications.topics.webhook}",
            groupId = "${notifications.consumer.webhook-group}",
            concurrency = "${notifications.consumer.concurrency}")
    void onMessage(ConsumerRecord<String, String> record) {
        delivery.deliver(parser.parse(record), sender);
    }

    @DltHandler
    void onDeadLetter(ConsumerRecord<String, String> record) {
        deadLetters.handle(record);
    }
}
