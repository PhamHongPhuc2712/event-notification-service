package com.eventnotification.worker.retry;

import com.eventnotification.worker.config.WorkerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Clock;
import java.util.function.Consumer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/** Plugs per-record jitter into the retry topics that {@link NotificationRetryableTopic} sets up. */
@Configuration
class RetryTopicConfig extends RetryTopicConfigurationSupport {

    private final WorkerProperties properties;
    private final Clock clock;

    RetryTopicConfig(WorkerProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
        RetryJitter jitter = new RetryJitter(properties.retry(), clock, jitterRandom());
        return factory -> factory.setHeadersFunction(jitter);
    }

    /**
     * Random numbers for the jitter, from the calling thread's {@link ThreadLocalRandom}, which
     * is safe to use from every consumer thread. Not {@link RandomGenerator#getDefault()}: its
     * algorithm lives in the jdk.random module, which the JRE container image does not include.
     */
    static RandomGenerator jitterRandom() {
        return () -> ThreadLocalRandom.current().nextLong();
    }

    /** Wakes paused retry-topic partitions when their next record is due. */
    @Bean
    TaskScheduler retryTopicScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("retry-topic-");
        return scheduler;
    }
}
