package com.eventnotification.mocksms.faults;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * How often each fault happens, as a probability per request.
 *
 * @param failureRate  respond 503 without accepting the message
 * @param timeoutRate  accept the message, then respond only after {@code timeoutDelay}, by which
 *                     time the caller has given up. The caller cannot tell this from a failure.
 * @param slowRate     accept the message and respond after {@code slowDelay}
 */
@Validated
@ConfigurationProperties("mock-sms.faults")
public record FaultSettings(
        @DecimalMin("0.0") @DecimalMax("1.0") double failureRate,
        @DecimalMin("0.0") @DecimalMax("1.0") double timeoutRate,
        @DecimalMin("0.0") @DecimalMax("1.0") double slowRate,
        @NotNull Duration timeoutDelay,
        @NotNull Duration slowDelay) {

    @JsonIgnore
    @AssertTrue(message = "the fault rates must add up to at most 1")
    public boolean isTotalRateAtMostOne() {
        return failureRate + timeoutRate + slowRate <= 1.0;
    }
}
