package com.eventnotification.worker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;

/** Reads a topic from the start until matching records show up. */
public final class TopicReader {

    private TopicReader() {
    }

    public static ConsumerRecord<String, String> awaitRecord(String bootstrapServers, String topic,
                                                             Predicate<ConsumerRecord<String, String>> match) {
        return awaitRecords(bootstrapServers, topic, match, 1).getFirst();
    }

    public static List<ConsumerRecord<String, String>> awaitRecords(String bootstrapServers, String topic,
                                                                    Predicate<ConsumerRecord<String, String>> match,
                                                                    int count) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(250))) {
                    if (match.test(r)) {
                        found.add(r);
                    }
                }
                return found.size() >= count;
            });
        }
        return found;
    }
}
