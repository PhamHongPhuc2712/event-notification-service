package com.eventnotification.mocksms.sms;

import com.eventnotification.mocksms.faults.FaultInjector;
import com.eventnotification.mocksms.faults.FaultSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class SmsControllerTest {

    private static final String SMS = """
            {"to":"+6591234567","body":"Your code is 123456"}""";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SmsStore store;

    @Autowired
    private FaultInjector faults;

    private FaultSettings original;

    @BeforeEach
    void noFaults() {
        original = faults.settings();
        setFaults(0, 0, 0);
        store.reset();
    }

    @AfterEach
    void restoreFaults() {
        faults.update(original);
    }

    @Test
    void acceptsMessage() throws Exception {
        send("key-1", SMS)
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("accepted"))
                .andExpect(header().doesNotExist(SmsController.REPLAYED));

        assertThat(store.messages()).singleElement().satisfies(m -> {
            assertThat(m.idempotencyKey()).isEqualTo("key-1");
            assertThat(m.to()).isEqualTo("+6591234567");
        });
    }

    @Test
    void repeatedKeyIsAcceptedOnceAndCountedAsDuplicate() throws Exception {
        String firstId = idOf(send("key-1", SMS).andReturn().getResponse().getContentAsString());

        send("key-1", SMS)
                .andExpect(status().isAccepted())
                .andExpect(header().string(SmsController.REPLAYED, "true"))
                .andExpect(jsonPath("$.id").value(firstId));

        assertThat(store.stats().accepted()).isEqualTo(1);
        assertThat(store.stats().duplicateRequests()).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentMessageIsRejected() throws Exception {
        send("key-1", SMS).andExpect(status().isAccepted());

        send("key-1", """
                {"to":"+6591234567","body":"Something else"}""")
                .andExpect(status().isUnprocessableContent());
    }

    @Test
    void missingIdempotencyKeyIsRejected() throws Exception {
        mockMvc.perform(post("/v1/messages").contentType(MediaType.APPLICATION_JSON).content(SMS))
                .andExpect(status().isBadRequest());
        assertThat(store.messages()).isEmpty();
    }

    @Test
    void invalidAndUnreachableNumbersAreRejected() throws Exception {
        send("key-1", """
                {"to":"12345","body":"b"}""").andExpect(status().isBadRequest());
        send("key-2", """
                {"to":"+9991234567","body":"b"}""").andExpect(status().isBadRequest());
        assertThat(store.messages()).isEmpty();
    }

    @Test
    void injectedFailureReturns503WithoutAcceptingTheMessage() throws Exception {
        setFaults(1, 0, 0);

        send("key-1", SMS).andExpect(status().isServiceUnavailable());

        assertThat(store.messages()).isEmpty();
        assertThat(store.stats().injectedFailures()).isEqualTo(1);
    }

    @Test
    void injectedTimeoutAcceptsTheMessageButRespondsLate() throws Exception {
        setFaults(0, 1, 0);

        long start = System.nanoTime();
        send("key-1", SMS).andExpect(status().isAccepted());

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(200));
        assertThat(store.messages()).hasSize(1);
        assertThat(store.stats().injectedTimeouts()).isEqualTo(1);
    }

    @Test
    void faultRatesCanBeChangedAtRuntimeAndAreValidated() throws Exception {
        mockMvc.perform(put("/admin/faults").contentType(MediaType.APPLICATION_JSON).content("""
                        {"failureRate":0.5,"timeoutRate":0.25,"slowRate":0.1,"timeoutDelay":"PT5S","slowDelay":"PT1S"}"""))
                .andExpect(status().isOk());
        assertThat(faults.settings().failureRate()).isEqualTo(0.5);

        mockMvc.perform(put("/admin/faults").contentType(MediaType.APPLICATION_JSON).content("""
                        {"failureRate":0.9,"timeoutRate":0.9,"slowRate":0,"timeoutDelay":"PT5S","slowDelay":"PT1S"}"""))
                .andExpect(status().isBadRequest());
    }

    private void setFaults(double failure, double timeout, double slow) {
        faults.update(new FaultSettings(failure, timeout, slow, Duration.ofMillis(200), Duration.ofMillis(50)));
    }

    private ResultActions send(String key, String body) throws Exception {
        return mockMvc.perform(post("/v1/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .header(SmsController.IDEMPOTENCY_KEY, key)
                .content(body));
    }

    private static String idOf(String json) {
        return json.replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");
    }
}
