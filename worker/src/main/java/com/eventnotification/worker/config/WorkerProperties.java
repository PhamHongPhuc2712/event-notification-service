package com.eventnotification.worker.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

@Validated
@ConfigurationProperties("notifications")
public record WorkerProperties(@Valid @NotNull Topics topics, @Valid @NotNull Consumer consumer,
                               @Valid @NotNull Mail mail, @Valid @NotNull Sms sms,
                               @Valid @NotNull Delivery delivery, @Valid @NotNull Retry retry,
                               @Valid @NotNull RateLimit rateLimit) {

    public record Topics(@NotBlank String email, @NotBlank String sms, @Min(1) int partitions) {
    }

    public record Consumer(@NotBlank String emailGroup, @NotBlank String smsGroup, @Min(1) int concurrency) {
    }

    public record Mail(@NotBlank @Email String from) {
    }

    /** The SMS provider's API. The timeouts bound how long one send can take. */
    public record Sms(@NotNull URI baseUrl, @NotNull Duration connectTimeout, @NotNull Duration readTimeout) {
    }

    /**
     * @param claimLease how long a worker's claim on a notification lasts. Must be longer than
     *                   the slowest possible send, or a slow send can be claimed and sent again.
     */
    public record Delivery(@NotNull Duration claimLease) {
    }

    /**
     * Non-blocking retries through retry topics. Retry {@code i} (from 0) waits
     * {@code min(initialDelay * multiplier^i, maxDelay)}, plus or minus a random {@code jitter}.
     *
     * @param attempts total attempts including the first, so {@code attempts - 1} retry topics
     */
    /**
     * Per-user rate limits.
     *
     * @param maxWait  the furthest ahead a send slot may be reserved; beyond it the notification
     *                 is SUPPRESSED as RATE_LIMITED
     * @param channels a bucket per channel name, such as SMS; a channel without one is unlimited
     */
    public record RateLimit(boolean enabled, @NotNull Duration maxWait,
                            @NotNull Map<String, @Valid @NotNull Bucket> channels) {
    }

    /** Allows bursts of {@code capacity}, then one notification per {@code refillPeriod}. */
    public record Bucket(@Min(1) int capacity, @NotNull Duration refillPeriod) {
    }

    public record Retry(@Min(2) int attempts, @NotNull Duration initialDelay, @DecimalMin("1.0") double multiplier,
                        @NotNull Duration maxDelay, @NotNull Duration jitter) {
    }
}
