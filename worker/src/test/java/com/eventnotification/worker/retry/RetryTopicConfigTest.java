package com.eventnotification.worker.retry;

import org.junit.jupiter.api.Test;

import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;

class RetryTopicConfigTest {

    @Test
    void jitterDoesNotNeedTheJdkRandomModuleWhichTheJreImageLacks() {
        // RandomGenerator.getDefault() comes from the jdk.random module. The eclipse-temurin JRE
        // image does not ship it, so the worker failed to start in Docker but not on a full JDK.
        RandomGenerator random = RetryTopicConfig.jitterRandom();

        assertThat(random.getClass().getModule().getName()).isNotEqualTo("jdk.random");
        assertThat(random.nextLong(-1_000, 1_001)).isBetween(-1_000L, 1_000L);
    }
}
