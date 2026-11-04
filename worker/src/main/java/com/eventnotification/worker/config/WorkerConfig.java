package com.eventnotification.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
class WorkerConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
