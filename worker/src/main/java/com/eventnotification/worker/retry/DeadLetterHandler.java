package com.eventnotification.worker.retry;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.MessageParser;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.NotificationStatusRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Optional;

/**
 * Handles records that reached a dead-letter topic. The notification is marked FAILED with
 * {@link FailureCodes#RETRIES_EXHAUSTED}, so it never sits in QUEUED forever, and the record
 * stays in the topic for {@link DeadLetterReplayer} to send again later.
 *
 * <p>Each one is counted in {@code notification_dead_letters_total{topic}}, malformed or not.
 */
@Component
public class DeadLetterHandler {

    public static final String DEAD_LETTERS = "notification.dead.letters";
    /** HIGH-priority notifications use the channel's topic with this suffix; see the api module's Priority. */
    private static final String HIGH_PRIORITY_SUFFIX = ".high";

    private static final Logger log = LoggerFactory.getLogger(DeadLetterHandler.class);

    private final MessageParser parser;
    private final NotificationStatusRepository statuses;
    private final MeterRegistry registry;
    private final Clock clock;

    public DeadLetterHandler(MessageParser parser, NotificationStatusRepository statuses, WorkerProperties properties,
                             MeterRegistry registry, Clock clock) {
        this.parser = parser;
        this.statuses = statuses;
        this.registry = registry;
        this.clock = clock;
        // Start every dead-letter topic at zero: Prometheus increase() cannot see a series that
        // first appears at 1, so the first dead letter on a fresh stack would not show.
        WorkerProperties.Topics topics = properties.topics();
        for (String topic : List.of(topics.email(), topics.sms(), topics.webhook())) {
            counter(topic + DeadLetterReplayer.DLT_SUFFIX);
            counter(topic + HIGH_PRIORITY_SUFFIX + DeadLetterReplayer.DLT_SUFFIX);
        }
    }

    public void handle(ConsumerRecord<String, String> record) {
        // Counted before marking FAILED, which keeps failing while the database is down: a
        // redelivered dead letter is counted again, so the rate shows the handler is stuck.
        counter(record.topic()).increment();
        String cause = failureCause(record);
        Optional<NotificationMessage> message = parser.tryParse(record);
        if (message.isEmpty()) {
            log.error("Dead-lettered malformed record at {}-{}@{} ({})", record.topic(), record.partition(),
                    record.offset(), cause);
            return;
        }
        boolean marked = statuses.markFailed(message.get().notificationId(), FailureCodes.RETRIES_EXHAUSTED,
                clock.instant());
        log.error("Dead-lettered notification {} after its last retry ({}); marked FAILED: {}",
                message.get().notificationId(), cause, marked);
    }

    private Counter counter(String topic) {
        return Counter.builder(DEAD_LETTERS)
                .description("Records that reached a dead-letter topic")
                .tag("topic", topic)
                .register(registry);
    }

    /**
     * The exception class from the last attempt. Retry topics use the {@code kafka_exception-*}
     * header names; the cause is more useful than the listener wrapper around it.
     */
    static String failureCause(ConsumerRecord<?, ?> record) {
        return header(record, KafkaHeaders.EXCEPTION_CAUSE_FQCN)
                .or(() -> header(record, KafkaHeaders.EXCEPTION_FQCN))
                .or(() -> header(record, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .orElse("unknown");
    }

    private static Optional<String> header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? Optional.empty() : Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }
}
