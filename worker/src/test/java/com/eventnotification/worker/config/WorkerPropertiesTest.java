package com.eventnotification.worker.config;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerPropertiesTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void recordingASendMustGiveUpBeforeTheClaimExpires() {
        // Otherwise another worker can claim the notification while this one still retries
        // marking it SENT, find it QUEUED, and send it again.
        var tooLong = new WorkerProperties.Delivery(Duration.ofSeconds(10), Duration.ofSeconds(30));
        var withinLease = new WorkerProperties.Delivery(Duration.ofSeconds(60), Duration.ofSeconds(30));

        assertThat(validator.validate(tooLong)).extracting(v -> v.getMessage())
                .containsExactly("record-sent-for must be shorter than claim-lease");
        assertThat(validator.validate(withinLease)).isEmpty();
    }
}
