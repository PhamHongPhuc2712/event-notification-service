package com.eventnotification.api.config;

import com.eventnotification.api.notification.Priority;
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

    /** HIGH-priority topics have their own consumers, so a backlog on the normal topic cannot delay them. */
    @Bean
    NewTopic emailHighPriorityTopic(NotificationProperties properties) {
        return topic(Priority.HIGH.topic(properties.topics().email()), properties);
    }

    @Bean
    NewTopic smsHighPriorityTopic(NotificationProperties properties) {
        return topic(Priority.HIGH.topic(properties.topics().sms()), properties);
    }

    private static NewTopic topic(String name, NotificationProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.topics().partitions())
                .replicas(1)
                .build();
    }
}
