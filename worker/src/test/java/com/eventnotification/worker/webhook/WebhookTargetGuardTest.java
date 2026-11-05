package com.eventnotification.worker.webhook;

import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebhookTargetGuardTest {

    private final WebhookTargetGuard guard = new WebhookTargetGuard(false);

    @ParameterizedTest
    @ValueSource(strings = {
            "https://127.0.0.1/hook",
            "https://localhost/hook",
            "https://10.0.0.5/hook",
            "https://172.16.1.1/hook",
            "https://192.168.1.10/hook",
            "https://169.254.169.254/latest/meta-data/", // cloud metadata
            "https://100.64.0.1/hook",                   // carrier-grade NAT
            "https://0.0.0.0/hook",
            "https://[::1]/hook",
            "https://[fd12:3456::1]/hook",               // IPv6 unique local
            "https://[::ffff:127.0.0.1]/hook"            // IPv4-mapped loopback
    })
    void blocksPrivateAndLocalAddresses(String url) {
        assertThatThrownBy(() -> guard.check(url))
                .isInstanceOfSatisfying(PermanentDeliveryException.class,
                        e -> assertThat(e.failureCode()).isEqualTo(FailureCodes.TARGET_NOT_ALLOWED));
    }

    @Test
    void blocksAPublicLookingNameThatResolvesToAPrivateAddress() throws Exception {
        InetAddress publicAddress = InetAddress.getByName("93.184.216.34");
        InetAddress internal = InetAddress.getByName("10.1.2.3");
        WebhookTargetGuard withDns = new WebhookTargetGuard(false,
                host -> new InetAddress[]{publicAddress, internal});

        assertThatThrownBy(() -> withDns.check("https://hooks.example.com/x"))
                .isInstanceOf(PermanentDeliveryException.class);
    }

    @Test
    void allowsAPublicHttpsTarget() throws Exception {
        InetAddress publicAddress = InetAddress.getByName("93.184.216.34");
        WebhookTargetGuard withDns = new WebhookTargetGuard(false, host -> new InetAddress[]{publicAddress});

        assertThat(withDns.check("https://hooks.example.com/x?a=1"))
                .isEqualTo(URI.create("https://hooks.example.com/x?a=1"));
    }

    @Test
    void blocksPlainHttp() {
        assertThatThrownBy(() -> guard.check("http://93.184.216.34/hook"))
                .isInstanceOf(PermanentDeliveryException.class);
    }

    @Test
    void blocksCredentialsInTheUrl() {
        assertThatThrownBy(() -> guard.check("https://user:pass@93.184.216.34/hook"))
                .isInstanceOf(PermanentDeliveryException.class);
    }

    @Test
    void unresolvableHostIsTransient() {
        WebhookTargetGuard withDns = new WebhookTargetGuard(false, host -> {
            throw new UnknownHostException(host);
        });

        assertThatThrownBy(() -> withDns.check("https://hooks.example.com/x"))
                .isInstanceOf(TransientDeliveryException.class);
    }

    @Test
    void localDevelopmentModeAllowsHttpToLocalhost() {
        assertThat(new WebhookTargetGuard(true).check("http://127.0.0.1:9000/hook"))
                .isEqualTo(URI.create("http://127.0.0.1:9000/hook"));
    }
}
