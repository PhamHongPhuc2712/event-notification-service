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
public record NotificationProperties(@Valid @NotNull Topics topics, @Valid @NotNull Outbox outbox,
                                     @Valid @NotNull Deferred deferred) {

    public record Topics(@NotBlank String email, @NotBlank String sms, @NotBlank String webhook,
                         @Min(1) int partitions) {
    }

    public record Outbox(@NotNull Duration pollInterval, @Min(1) int batchSize, @NotNull Duration sendTimeout,
                         @Valid @NotNull Cleanup cleanup) {

        /**
         * Deletes published rows once they are older than {@code retention}, so the table stays
         * small. Unpublished rows are never deleted.
         *
         * @param interval  how often the cleanup runs
         * @param retention how long a published row is kept, for looking up when it was published
         * @param batchSize rows deleted per statement, so one delete never holds locks for long
         */
        public record Cleanup(@NotNull Duration interval, @NotNull Duration retention, @Min(1) int batchSize) {
        }
    }

    /** How often, and how many at a time, DEFERRED notifications are released after quiet hours. */
    public record Deferred(@NotNull Duration pollInterval, @Min(1) int batchSize) {
    }
}
