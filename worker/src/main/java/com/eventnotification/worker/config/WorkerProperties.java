package com.eventnotification.worker.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("notifications")
public record WorkerProperties(@Valid @NotNull Topics topics, @Valid @NotNull Consumer consumer,
                               @Valid @NotNull Mail mail, @Valid @NotNull Delivery delivery) {

    public record Topics(@NotBlank String email) {
    }

    public record Consumer(@NotBlank String emailGroup, @Min(1) int concurrency) {
    }

    public record Mail(@NotBlank @Email String from) {
    }

    /**
     * @param claimLease how long a worker's claim on a notification lasts. Must be longer than
     *                   the SMTP timeouts, or a slow send can be claimed and sent again.
     */
    public record Delivery(@NotNull Duration claimLease) {
    }
}
