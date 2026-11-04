package com.eventnotification.worker.retry;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.MalformedMessageException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.SingleRecordHeader;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;

import java.math.BigInteger;
import java.time.Clock;
import java.util.function.BiFunction;
import java.util.random.RandomGenerator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sets a jittered due time on each record published to a retry topic.
 *
 * <p>Spring Kafka draws {@code @BackOff} jitter once per topic at startup, so every record in a
 * retry topic would wait exactly the same delay. When a provider outage fails thousands of
 * records at once, they would all retry at the same instant and knock it over again. This
 * replaces the due-time header with one drawn per record. The header is a
 * {@link SingleRecordHeader}, so it replaces the one Spring computed.
 */
public class RetryJitter implements BiFunction<ConsumerRecord<?, ?>, Exception, Headers> {

    static final String DUE_TIME_HEADER = RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP;
    private static final Pattern RETRY_TOPIC = Pattern.compile(".*-retry-(\\d+)");

    private final WorkerProperties.Retry retry;
    private final Clock clock;
    private final RandomGenerator random;

    public RetryJitter(WorkerProperties.Retry retry, Clock clock, RandomGenerator random) {
        this.retry = retry;
        this.clock = clock;
        this.random = random;
    }

    @Override
    public Headers apply(ConsumerRecord<?, ?> record, Exception exception) {
        Headers headers = new RecordHeaders();
        int next = nextRetryIndex(record.topic());
        if (next > lastRetryIndex() || isMalformed(exception)) {
            return headers; // going to the dead-letter topic, which has no delay
        }
        long dueAt = clock.millis() + delayMillis(next);
        headers.add(new SingleRecordHeader(DUE_TIME_HEADER, BigInteger.valueOf(dueAt).toByteArray()));
        return headers;
    }

    /** {@code min(initialDelay * multiplier^index, maxDelay)}, moved by up to {@code jitter} either way. */
    long delayMillis(int retryIndex) {
        double base = retry.initialDelay().toMillis() * Math.pow(retry.multiplier(), retryIndex);
        long capped = (long) Math.min(base, retry.maxDelay().toMillis());
        long jitter = retry.jitter().toMillis();
        long offset = jitter == 0 ? 0 : random.nextLong(-jitter, jitter + 1);
        return Math.max(0, capped + offset);
    }

    /** The main topic retries into {@code -retry-0}; {@code -retry-N} retries into {@code -retry-(N+1)}. */
    static int nextRetryIndex(String topic) {
        Matcher m = RETRY_TOPIC.matcher(topic);
        return m.matches() ? Integer.parseInt(m.group(1)) + 1 : 0;
    }

    private int lastRetryIndex() {
        return retry.attempts() - 2;
    }

    private static boolean isMalformed(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof MalformedMessageException) {
                return true;
            }
        }
        return false;
    }
}
