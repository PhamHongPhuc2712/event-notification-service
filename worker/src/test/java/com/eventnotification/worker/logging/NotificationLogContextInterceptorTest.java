package com.eventnotification.worker.logging;

import com.eventnotification.worker.delivery.MessageParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class NotificationLogContextInterceptorTest {

    private final NotificationLogContextInterceptor interceptor =
            new NotificationLogContextInterceptor(new MessageParser(JsonMapper.builder().build()));
    @SuppressWarnings("unchecked")
    private final Consumer<Object, Object> consumer = mock(Consumer.class);

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void putsTheNotificationIdInTheLogContextForTheWholeRecord() {
        UUID id = UUID.randomUUID();
        ConsumerRecord<Object, Object> record = record("{\"notificationId\":\"" + id + "\",\"channel\":\"SMS\"}");

        assertThat(interceptor.intercept(record, consumer)).isSameAs(record);
        assertThat(MDC.get(NotificationLogContext.NOTIFICATION_ID)).isEqualTo(id.toString());

        interceptor.afterRecord(record, consumer);
        assertThat(MDC.get(NotificationLogContext.NOTIFICATION_ID)).isNull();
    }

    @Test
    void passesMalformedRecordsThroughWithoutAnId() {
        MDC.put(NotificationLogContext.NOTIFICATION_ID, "left-over-from-an-earlier-record");
        ConsumerRecord<Object, Object> record = record("not json");

        assertThat(interceptor.intercept(record, consumer)).isSameAs(record);
        assertThat(MDC.get(NotificationLogContext.NOTIFICATION_ID)).isNull();
    }

    @Test
    void ignoresRecordsWhoseValueIsNotAString() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("notifications.sms", 0, 0, "user-1", 42);

        assertThat(interceptor.intercept(record, consumer)).isSameAs(record);
        assertThat(MDC.get(NotificationLogContext.NOTIFICATION_ID)).isNull();
    }

    private static ConsumerRecord<Object, Object> record(String value) {
        return new ConsumerRecord<>("notifications.sms", 0, 7, "user-1", value);
    }
}
