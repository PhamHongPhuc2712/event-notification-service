package com.eventnotification.worker;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;

/**
 * Real Postgres, Kafka, Redis and Mailpit for tests, using the same image versions as
 * infra/docker-compose.yml, plus a scriptable fake SMS provider.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    public static final int MAILPIT_SMTP_PORT = 1025;
    public static final int MAILPIT_HTTP_PORT = 8025;

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:16.15-alpine");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.3.2");
    }

    @Bean
    @ServiceConnection(name = "redis")
    GenericContainer<?> redis() {
        return new GenericContainer<>("redis:7.4.12-alpine").withExposedPorts(6379);
    }

    @Bean
    GenericContainer<?> mailpit() {
        return new GenericContainer<>("axllent/mailpit:v1.31.6")
                .withExposedPorts(MAILPIT_SMTP_PORT, MAILPIT_HTTP_PORT);
    }

    @Bean(destroyMethod = "close")
    FakeSmsProvider fakeSmsProvider() throws IOException {
        return new FakeSmsProvider();
    }

    @Bean
    DynamicPropertyRegistrar providerProperties(GenericContainer<?> mailpit, FakeSmsProvider sms) {
        return registry -> {
            registry.add("spring.mail.host", mailpit::getHost);
            registry.add("spring.mail.port", () -> mailpit.getMappedPort(MAILPIT_SMTP_PORT));
            registry.add("notifications.sms.base-url", () -> sms.baseUrl().toString());
        };
    }
}
