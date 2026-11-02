package com.eventnotification.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
class WorkerConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Retries a failing record in place (keeping per-user order) with exponential backoff,
     * for example while Postgres is briefly down. After about five minutes the record is
     * logged and skipped. Weeks 3-4 replace the skip with retry topics and a dead-letter topic.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000, 2.0);
        backOff.setMaxInterval(30_000);
        backOff.setMaxElapsedTime(300_000);
        return new DefaultErrorHandler(backOff);
    }
}
