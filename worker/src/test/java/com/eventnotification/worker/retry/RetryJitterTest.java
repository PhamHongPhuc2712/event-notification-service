package com.eventnotification.worker.retry;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.MalformedMessageException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.SingleRecordHeader;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;

class RetryJitterTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");

    // 4 attempts: retry-0 waits 2s, retry-1 6s, retry-2 10s (capped), each +/- 1s.
    private final WorkerProperties.Retry retry = new WorkerProperties.Retry(4, Duration.ofSeconds(2), 3,
            Duration.ofSeconds(10), Duration.ofSeconds(1));
    private final RetryJitter jitter = new RetryJitter(retry, Clock.fixed(NOW, ZoneOffset.UTC),
            RandomGenerator.of("L64X128MixRandom"));

    @Test
    void delaysGrowExponentiallyUpToTheCapWithinTheJitterBand() {
        for (int i = 0; i < 200; i++) {
            assertThat(jitter.delayMillis(0)).isBetween(1_000L, 3_000L);
            assertThat(jitter.delayMillis(1)).isBetween(5_000L, 7_000L);
            assertThat(jitter.delayMillis(2)).isBetween(9_000L, 11_000L);
            assertThat(jitter.delayMillis(5)).isBetween(9_000L, 11_000L);
        }
    }

    @Test
    void recordsFailingTogetherGetDifferentDueTimes() {
        Set<Long> dueTimes = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            dueTimes.add(dueTime(jitter.apply(record("notifications.sms"), transientError())));
        }
        assertThat(dueTimes).hasSizeGreaterThan(40);
    }

    @Test
    void dueTimeReplacesSpringsHeaderAndUsesItsEncoding() {
        Headers headers = jitter.apply(record("notifications.sms-retry-0"), transientError());

        Header header = headers.lastHeader(RetryJitter.DUE_TIME_HEADER);
        assertThat(header).isInstanceOf(SingleRecordHeader.class);
        long delay = dueTime(headers) - NOW.toEpochMilli();
        assertThat(delay).isBetween(5_000L, 7_000L);
    }

    @Test
    void addsNoDueTimeWhenTheNextHopIsTheDeadLetterTopic() {
        assertThat(jitter.apply(record("notifications.sms-retry-2"), transientError())
                .lastHeader(RetryJitter.DUE_TIME_HEADER)).isNull();
        assertThat(jitter.apply(record("notifications.sms"), new MalformedMessageException("bad", null))
                .lastHeader(RetryJitter.DUE_TIME_HEADER)).isNull();
    }

    @Test
    void findsTheNextRetryIndexFromTheTopicName() {
        assertThat(RetryJitter.nextRetryIndex("notifications.email")).isZero();
        assertThat(RetryJitter.nextRetryIndex("notifications.email-retry-0")).isEqualTo(1);
        assertThat(RetryJitter.nextRetryIndex("notifications.email-retry-11")).isEqualTo(12);
    }

    private static ConsumerRecord<String, String> record(String topic) {
        return new ConsumerRecord<>(topic, 0, 0, "user-1", "{}");
    }

    private static TransientDeliveryException transientError() {
        return new TransientDeliveryException("provider down", null);
    }

    private static long dueTime(Headers headers) {
        return new BigInteger(headers.lastHeader(RetryJitter.DUE_TIME_HEADER).value()).longValue();
    }
}
