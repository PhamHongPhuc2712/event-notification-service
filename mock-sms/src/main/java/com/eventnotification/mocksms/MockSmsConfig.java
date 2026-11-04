package com.eventnotification.mocksms;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
class MockSmsConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
