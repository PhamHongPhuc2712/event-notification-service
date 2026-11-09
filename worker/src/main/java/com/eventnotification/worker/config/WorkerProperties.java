package com.eventnotification.worker.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

@Validated
@ConfigurationProperties("notifications")
public record WorkerProperties(@Valid @NotNull Topics topics, @Valid @NotNull Consumer consumer,
                               @Valid @NotNull Mail mail, @Valid @NotNull Sms sms, @Valid @NotNull Webhook webhook,
                               @Valid @NotNull Delivery delivery, @Valid @NotNull Retry retry,
                               @Valid @NotNull RateLimit rateLimit) {

    public record Topics(@NotBlank String email, @NotBlank String sms, @NotBlank String webhook,
                         @Min(1) int partitions) {
    }

    /**
     * @param highPriorityConcurrency consumer threads per HIGH-priority topic. These are separate
     *                                from the normal topic's threads, so a backlog there cannot
     *                                hold up a password reset.
     */
    public record Consumer(@NotBlank String emailGroup, @NotBlank String smsGroup, @NotBlank String webhookGroup,
                           @Min(1) int concurrency, @Min(1) int highPriorityConcurrency) {
    }

    public record Mail(@NotBlank @Email String from) {
    }

    /** The SMS provider's API. The timeouts bound how long one send can take. */
    public record Sms(@NotNull URI baseUrl, @NotNull Duration connectTimeout, @NotNull Duration readTimeout) {
    }

    /**
     * Outgoing webhooks.
     *
     * @param signingSecret       {@code whsec_} plus base64 of 24 to 64 random bytes, from the environment
     * @param allowPrivateTargets allow http and private addresses such as localhost; for local
     *                            development only, since it lets webhooks reach internal services
     */
    public record Webhook(@NotBlank @Pattern(regexp = "whsec_.+", message = "must start with whsec_") String signingSecret,
                          @NotNull Duration connectTimeout, @NotNull Duration readTimeout,
                          boolean allowPrivateTargets) {
    }

    /**
     * @param claimLease    how long a worker's claim on a notification lasts. Must be longer than
     *                      the slowest possible send, or a slow send can be claimed and sent again.
     * @param recordSentFor how long to keep retrying to mark a notification SENT after the send
     *                      succeeded, for example while the database is briefly unreachable.
     *                      Shorter than the claim lease, so no other worker takes it meanwhile.
     */
    public record Delivery(@NotNull Duration claimLease, @NotNull Duration recordSentFor) {
    }

    /**
     * Per-user rate limits.
     *
     * @param maxWait  the furthest ahead a send slot may be reserved; beyond it the notification
     *                 is SUPPRESSED as RATE_LIMITED
     * @param channels             a bucket per channel name, such as SMS; a channel without one is unlimited
     * @param highPriorityChannels the same for HIGH-priority notifications, which have their own buckets
     */
    public record RateLimit(boolean enabled, @NotNull Duration maxWait,
                            @NotNull Map<String, @Valid @NotNull Bucket> channels,
                            @NotNull Map<String, @Valid @NotNull Bucket> highPriorityChannels) {
    }

    /** Allows bursts of {@code capacity}, then one notification per {@code refillPeriod}. */
    public record Bucket(@Min(1) int capacity, @NotNull Duration refillPeriod) {
    }

    /**
     * Non-blocking retries through retry topics. Retry {@code i} (from 0) waits
     * {@code min(initialDelay * multiplier^i, maxDelay)}, plus or minus a random {@code jitter}.
     *
     * @param attempts    total attempts including the first, so {@code attempts - 1} retry topics
     * @param concurrency consumer threads for each retry and dead-letter topic. They carry a small
     *                    share of the traffic, so they need far fewer than the main topics.
     */
    public record Retry(@Min(2) int attempts, @NotNull Duration initialDelay, @DecimalMin("1.0") double multiplier,
                        @NotNull Duration maxDelay, @NotNull Duration jitter, @Min(1) int concurrency) {
    }
}
