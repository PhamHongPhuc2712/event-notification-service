package com.eventnotification.worker.sms;

import com.eventnotification.worker.delivery.DeliveryService;
import com.eventnotification.worker.delivery.MessageParser;
import com.eventnotification.worker.retry.DeadLetterHandler;
import com.eventnotification.worker.retry.NotificationRetryableTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the HIGH-priority sms topic with its own consumer group and threads, so these
 * notifications never wait behind the normal topic's backlog.
 */
@Component
class SmsHighPriorityListener {

    private final DeliveryService delivery;
    private final SmsSender sender;
    private final MessageParser parser;
    private final DeadLetterHandler deadLetters;

    SmsHighPriorityListener(DeliveryService delivery, SmsSender sender, MessageParser parser,
                              DeadLetterHandler deadLetters) {
        this.delivery = delivery;
        this.sender = sender;
        this.parser = parser;
        this.deadLetters = deadLetters;
    }

    @NotificationRetryableTopic
    @KafkaListener(
            id = "sms-high-worker",
            topics = "${notifications.topics.sms}.high",
            groupId = "${notifications.consumer.sms-group}-high",
            concurrency = "${notifications.consumer.high-priority-concurrency}")
    void onMessage(ConsumerRecord<String, String> record) {
        delivery.deliver(parser.parse(record), sender);
    }

    @DltHandler
    void onDeadLetter(ConsumerRecord<String, String> record) {
        deadLetters.handle(record);
    }
}
