package com.eventnotification.worker.webhook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class WebhookSignerTest {

    @Test
    void matchesTheStandardWebhooksSpecExample() {
        // The example from the Standard Webhooks spec, so off-the-shelf verifiers accept our signatures.
        WebhookSigner signer = new WebhookSigner("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw");

        String signature = signer.sign("msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330, "{\"test\": 2432232314}");

        assertThat(signature).isEqualTo("v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=");
    }

    @Test
    void signatureChangesWithTheIdTimestampOrBody() {
        WebhookSigner signer = new WebhookSigner("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw");
        String original = signer.sign("msg_1", 1000, "{}");

        assertThat(signer.sign("msg_2", 1000, "{}")).isNotEqualTo(original);
        assertThat(signer.sign("msg_1", 1001, "{}")).isNotEqualTo(original);
        assertThat(signer.sign("msg_1", 1000, "{ }")).isNotEqualTo(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",  // missing prefix
            "whsec_not base64!",                 // not base64
            "whsec_c2hvcnQ=",                    // only 5 bytes
            "change-me"                          // the .env.example placeholder
    })
    void rejectsSecretsThatAreMalformedOrTooShort(String secret) {
        assertThatIllegalArgumentException().isThrownBy(() -> new WebhookSigner(secret));
    }
}
