package com.eventnotification.api.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("notifications")
public record NotificationProperties(@Valid @NotNull Topics topics, @Valid @NotNull Outbox outbox) {

    public record Topics(@NotBlank String email, @Min(1) int partitions) {
    }

    public record Outbox(@NotNull Duration pollInterval, @Min(1) int batchSize, @NotNull Duration sendTimeout) {
    }
}
