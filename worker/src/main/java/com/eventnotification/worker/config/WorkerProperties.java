package com.eventnotification.worker.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("notifications")
public record WorkerProperties(@Valid @NotNull Topics topics, @Valid @NotNull Consumer consumer,
                               @Valid @NotNull Mail mail) {

    public record Topics(@NotBlank String email) {
    }

    public record Consumer(@NotBlank String emailGroup, @Min(1) int concurrency) {
    }

    public record Mail(@NotBlank @Email String from) {
    }
}
