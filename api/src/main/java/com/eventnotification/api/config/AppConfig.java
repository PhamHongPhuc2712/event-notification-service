package com.eventnotification.api.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
class AppConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Created on startup if missing. Partitioned by user ID so each user's notifications stay in order. */
    @Bean
    NewTopic emailTopic(NotificationProperties properties) {
        return topic(properties.topics().email(), properties);
    }

    @Bean
    NewTopic smsTopic(NotificationProperties properties) {
        return topic(properties.topics().sms(), properties);
    }

    private static NewTopic topic(String name, NotificationProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.topics().partitions())
                .replicas(1)
                .build();
    }
}
