package com.eventnotification.worker.retry;

import com.eventnotification.worker.delivery.MalformedMessageException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Non-blocking retries for a notification listener: a failed record moves to
 * {@code <topic>-retry-0}, then {@code -retry-1} and so on, and finally to {@code <topic>-dlt}.
 * The main partition keeps flowing while the record waits.
 *
 * <p>Topics are suffixed by index rather than delay because {@link RetryJitter} randomizes each
 * record's delay. Malformed messages skip the retries and go straight to the dead-letter topic.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@RetryableTopic(
        attempts = "${notifications.retry.attempts}",
        backOff = @BackOff(
                delayString = "${notifications.retry.initial-delay}",
                multiplierString = "${notifications.retry.multiplier}",
                maxDelayString = "${notifications.retry.max-delay}"),
        numPartitions = "${notifications.topics.partitions}",
        exclude = MalformedMessageException.class,
        traversingCauses = "true",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
public @interface NotificationRetryableTopic {
}
