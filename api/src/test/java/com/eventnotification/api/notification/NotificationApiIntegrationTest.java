package com.eventnotification.api.notification;

import com.eventnotification.api.TestcontainersConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class NotificationApiIntegrationTest {

    private static final String EMAIL_TOPIC = "notifications.email";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaContainer kafka;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void createReturnsAcceptedWithLocationAndQueuedStatus() throws Exception {
        String userId = uniqueUserId();

        MvcResult result = mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(emailRequest(userId, "Your order shipped")))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.userId").value(userId))
                .andReturn();

        String id = jsonMapper.readTree(result.getResponse().getContentAsString()).get("id").asString();
        assertThat(result.getResponse().getHeader("Location")).endsWith("/notifications/" + id);

        mockMvc.perform(get("/notifications/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.channel").value("EMAIL"))
                .andExpect(jsonPath("$.recipient").value("alice@example.com"))
                .andExpect(jsonPath("$.subject").value("Your order shipped"));
    }

    @Test
    void createPublishesMessageToKafkaKeyedByUserIdAndMarksOutboxRowPublished() throws Exception {
        String userId = uniqueUserId();
        UUID id = createEmail(userId, "Password reset");

        ConsumerRecord<String, String> record = awaitRecordFor(id);

        assertThat(record.key()).isEqualTo(userId);
        JsonNode message = jsonMapper.readTree(record.value());
        assertThat(message.get("notificationId").asString()).isEqualTo(id.toString());
        assertThat(message.get("userId").asString()).isEqualTo(userId);
        assertThat(message.get("subject").asString()).isEqualTo("Password reset");
        assertThat(message.get("body").asString()).isEqualTo("Hello from the test");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox WHERE aggregate_id = :id")
                        .param("id", id)
                        .query(Boolean.class)
                        .single()).isTrue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // missing body
            """
            {"userId":"u1","channel":"EMAIL","recipient":"a@example.com","subject":"s"}""",
            // invalid email
            """
            {"userId":"u1","channel":"EMAIL","recipient":"not-an-email","subject":"s","body":"b"}""",
            // email without subject
            """
            {"userId":"u1","channel":"EMAIL","recipient":"a@example.com","body":"b"}""",
            // illegal characters in user ID
            """
            {"userId":"u 1;drop","channel":"EMAIL","recipient":"a@example.com","subject":"s","body":"b"}""",
            // line break in subject (header injection)
            """
            {"userId":"u1","channel":"EMAIL","recipient":"a@example.com","subject":"hi\\r\\nBcc: x@evil.test","body":"b"}""",
            // null character in body
            """
            {"userId":"u1","channel":"EMAIL","recipient":"a@example.com","subject":"s","body":"a\\u0000b"}""",
            // unknown channel
            """
            {"userId":"u1","channel":"PIGEON","recipient":"a@example.com","subject":"s","body":"b"}""",
            // malformed JSON
            "{not json"
    })
    void createRejectsInvalidRequests(String body) throws Exception {
        mockMvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE));
    }

    @Test
    void validationErrorsListEachInvalidField() throws Exception {
        mockMvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"userId":"u1","channel":"EMAIL","recipient":"not-an-email","subject":"s"}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.field == 'recipient')]").exists())
                .andExpect(jsonPath("$.errors[?(@.field == 'body')]").exists());
    }

    @Test
    void getUnknownNotificationReturnsNotFound() throws Exception {
        mockMvc.perform(get("/notifications/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void getWithMalformedIdReturnsBadRequest() throws Exception {
        mockMvc.perform(get("/notifications/not-a-uuid"))
                .andExpect(status().isBadRequest());
    }

    private UUID createEmail(String userId, String subject) throws Exception {
        MvcResult result = mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(emailRequest(userId, subject)))
                .andExpect(status().isAccepted())
                .andReturn();
        return UUID.fromString(jsonMapper.readTree(result.getResponse().getContentAsString()).get("id").asString());
    }

    private String emailRequest(String userId, String subject) {
        return jsonMapper.writeValueAsString(Map.of(
                "userId", userId,
                "channel", "EMAIL",
                "recipient", "alice@example.com",
                "subject", subject,
                "body", "Hello from the test"));
    }

    private ConsumerRecord<String, String> awaitRecordFor(UUID notificationId) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(EMAIL_TOPIC));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(250))) {
                    if (r.value().contains(notificationId.toString())) {
                        found.set(r);
                    }
                }
                return found.get() != null;
            });
        }
        return Optional.ofNullable(found.get()).orElseThrow();
    }

    private static String uniqueUserId() {
        return "user-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
