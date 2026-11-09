package com.eventnotification.worker.logging;

import com.eventnotification.worker.delivery.MessageParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

/**
 * Puts the notification ID of each consumed record in the log context, for every listener and
 * retry topic.
 *
 * <p>An interceptor rather than code in the listeners, because the container calls
 * {@link #afterRecord} only after its error handler has run. So the lines Spring Kafka logs
 * while publishing a failed record to a retry or dead-letter topic carry the ID too.
 */
@Component
public class NotificationLogContextInterceptor implements RecordInterceptor<Object, Object> {

    private final MessageParser parser;

    public NotificationLogContextInterceptor(MessageParser parser) {
        this.parser = parser;
    }

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record,
                                                    Consumer<Object, Object> consumer) {
        MDC.remove(NotificationLogContext.NOTIFICATION_ID);
        if (record.value() instanceof String value) {
            parser.tryParse(value).ifPresent(message ->
                    MDC.put(NotificationLogContext.NOTIFICATION_ID, message.notificationId().toString()));
        }
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        MDC.remove(NotificationLogContext.NOTIFICATION_ID);
    }
}
