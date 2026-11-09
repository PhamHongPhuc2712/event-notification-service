package com.eventnotification.worker.retry;

import com.eventnotification.worker.delivery.MessageParser;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.NotificationStatusRepository;
import com.eventnotification.worker.logging.NotificationLogContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sends dead-lettered notifications through the main topic again, after the cause (say a
 * provider outage) is fixed.
 *
 * <p>For each record: requeue the notification, publish it to the main topic, then commit the
 * record's offset under the {@value #GROUP_ID} group. A crash between publishing and committing
 * replays the record twice, which the worker's claim and status check absorb. A crash before
 * publishing leaves the record uncommitted, so the next run picks it up.
 */
@Component
public class DeadLetterReplayer {

    public static final String GROUP_ID = "dlt-replay";
    static final String DLT_SUFFIX = "-dlt";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final int EMPTY_POLLS_BEFORE_DONE = 3;
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayer.class);

    /** @param skipped malformed records, which cannot be replayed; their offsets are committed */
    public record Result(int replayed, int skipped) {
    }

    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafka;
    private final MessageParser parser;
    private final NotificationStatusRepository statuses;
    private final Clock clock;

    public DeadLetterReplayer(ConsumerFactory<String, String> consumerFactory, KafkaTemplate<String, String> kafka,
                              MessageParser parser, NotificationStatusRepository statuses, Clock clock) {
        this.consumerFactory = consumerFactory;
        this.kafka = kafka;
        this.parser = parser;
        this.statuses = statuses;
        this.clock = clock;
    }

    /** Replays up to {@code limit} records from {@code dltTopic}, oldest first, into its main topic. */
    public Result replay(String dltTopic, int limit) {
        if (!dltTopic.endsWith(DLT_SUFFIX)) {
            throw new IllegalArgumentException("Not a dead-letter topic: " + dltTopic);
        }
        String mainTopic = dltTopic.substring(0, dltTopic.length() - DLT_SUFFIX.length());
        int replayed = 0;
        int skipped = 0;
        try (Consumer<String, String> consumer = createConsumer()) {
            // assign() rather than subscribe(): no group rebalance to wait for, and committed
            // offsets in the group still mark where the last run stopped.
            List<TopicPartition> partitions = consumer.partitionsFor(dltTopic).stream()
                    .map(p -> new TopicPartition(dltTopic, p.partition()))
                    .toList();
            consumer.assign(partitions);
            int emptyPolls = 0;
            while (replayed + skipped < limit && emptyPolls < EMPTY_POLLS_BEFORE_DONE) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                emptyPolls = records.isEmpty() ? emptyPolls + 1 : 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (replayed + skipped >= limit) {
                        break;
                    }
                    if (replayOne(record, mainTopic)) {
                        replayed++;
                    } else {
                        skipped++;
                    }
                    consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)));
                }
            }
        }
        log.info("Replayed {} records from {} into {}; skipped {} malformed", replayed, dltTopic, mainTopic, skipped);
        return new Result(replayed, skipped);
    }

    private boolean replayOne(ConsumerRecord<String, String> record, String mainTopic) {
        Optional<NotificationMessage> message = parser.tryParse(record);
        if (message.isEmpty()) {
            log.warn("Skipping malformed dead letter at {}-{}@{}", record.topic(), record.partition(), record.offset());
            return false;
        }
        // The log context covers the catch blocks too, so a failure is logged with the ID.
        try (MDC.MDCCloseable ignored = NotificationLogContext.put(message.get().notificationId())) {
            try {
                statuses.requeueExhausted(message.get().notificationId(), clock.instant());
                kafka.send(mainTopic, record.key(), record.value()).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.info("Replayed notification {} into {}", message.get().notificationId(), mainTopic);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while replaying " + message.get().notificationId(), e);
            } catch (ExecutionException | TimeoutException e) {
                log.error("Could not replay notification {} into {}", message.get().notificationId(), mainTopic, e);
                throw new IllegalStateException("Could not replay notification " + message.get().notificationId(), e);
            }
        }
        return true;
    }

    private Consumer<String, String> createConsumer() {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerFactory.createConsumer(GROUP_ID, null, "-replay", overrides);
    }
}
