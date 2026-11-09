package com.eventnotification.api.notification;

import com.eventnotification.api.TestcontainersConfiguration;
import com.eventnotification.api.idempotency.IdempotencyKey;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
@ExtendWith(OutputCaptureExtension.class)
class NotificationApiIntegrationTest {

    private static final String EMAIL_TOPIC = "notifications.email";
    private static final String SMS_TOPIC = "notifications.sms";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private KafkaContainer kafka;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private NotificationService service;

    @Autowired
    private MeterRegistry registry;

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
            // SMS to an email address
            """
            {"userId":"u1","channel":"SMS","recipient":"a@example.com","body":"b"}""",
            // SMS number without the + and country code
            """
            {"userId":"u1","channel":"SMS","recipient":"91234567","body":"b"}""",
            // email to a phone number
            """
            {"userId":"u1","channel":"EMAIL","recipient":"+6591234567","subject":"s","body":"b"}""",
            // webhook over plain http to a public host
            """
            {"userId":"u1","channel":"WEBHOOK","recipient":"http://example.com/hook","body":"b"}""",
            // webhook URL with credentials
            """
            {"userId":"u1","channel":"WEBHOOK","recipient":"https://user:pass@example.com/hook","body":"b"}""",
            // webhook that is not a URL
            """
            {"userId":"u1","channel":"WEBHOOK","recipient":"example.com/hook","body":"b"}""",
            // webhook with another scheme
            """
            {"userId":"u1","channel":"WEBHOOK","recipient":"ftp://example.com/hook","body":"b"}""",
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
    void smsIsAcceptedWithoutSubjectAndPublishedToTheSmsTopic() throws Exception {
        String userId = uniqueUserId();
        MvcResult result = mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(smsRequest(userId, "Your code is 123456")))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.channel").value("SMS"))
                .andExpect(jsonPath("$.recipient").value("+6591234567"))
                .andReturn();

        ConsumerRecord<String, String> record = awaitRecordFor(idOf(result), SMS_TOPIC);

        assertThat(record.key()).isEqualTo(userId);
        assertThat(jsonMapper.readTree(record.value()).get("channel").asString()).isEqualTo("SMS");
    }

    @Test
    void highPriorityGoesToTheChannelsHighPriorityTopic() throws Exception {
        String userId = uniqueUserId();
        MvcResult result = mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonMapper.writeValueAsString(Map.of(
                                "userId", userId, "channel", "SMS", "recipient", "+6591234567",
                                "body", "Your reset code is 123456", "priority", "HIGH"))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.priority").value("HIGH"))
                .andReturn();

        ConsumerRecord<String, String> record = awaitRecordFor(idOf(result), SMS_TOPIC + ".high");

        assertThat(jsonMapper.readTree(record.value()).get("priority").asString()).isEqualTo("HIGH");
    }

    @Test
    void webhookIsAcceptedAndPublishedToTheWebhookTopic() throws Exception {
        String userId = uniqueUserId();
        MvcResult result = mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonMapper.writeValueAsString(Map.of(
                                "userId", userId, "channel", "WEBHOOK",
                                "recipient", "https://hooks.example.com/orders?source=ens",
                                "subject", "order.shipped", "body", "{\"orderId\":42}"))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.channel").value("WEBHOOK"))
                .andReturn();

        ConsumerRecord<String, String> record = awaitRecordFor(idOf(result), "notifications.webhook");

        assertThat(record.key()).isEqualTo(userId);
    }

    @Test
    void priorityDefaultsToNormal() throws Exception {
        mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(smsRequest(uniqueUserId(), "Weekly digest")))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.priority").value("NORMAL"));
    }

    @Test
    void unknownPriorityIsRejected() throws Exception {
        mockMvc.perform(post("/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"userId":"u1","channel":"SMS","recipient":"+6591234567","body":"b","priority":"URGENT"}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void reusingIdempotencyKeyWithADifferentPriorityIsRejected() throws Exception {
        String userId = uniqueUserId();
        postWithKey(smsRequest(userId, "Code"), "prio-key").andExpect(status().isAccepted());

        postWithKey(jsonMapper.writeValueAsString(Map.of("userId", userId, "channel", "SMS",
                "recipient", "+6591234567", "body", "Code", "priority", "HIGH")), "prio-key")
                .andExpect(status().isUnprocessableContent());
    }

    @Test
    void channelSpecificErrorsAreReportedOnTheirFields() throws Exception {
        mockMvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON)
                        .content(jsonMapper.writeValueAsString(Map.of(
                                "userId", "u1", "channel", "SMS", "recipient", "12345", "body", "x".repeat(1_601)))))
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

    @Test
    void repeatedIdempotencyKeyReturnsTheSameNotificationAndCreatesOnlyOne() throws Exception {
        String userId = uniqueUserId();
        String body = emailRequest(userId, "Receipt");

        MvcResult first = postWithKey(body, "order-42-receipt")
                .andExpect(status().isAccepted())
                .andExpect(header().doesNotExist(NotificationController.REPLAYED_HEADER))
                .andReturn();
        MvcResult second = postWithKey(body, "order-42-receipt")
                .andExpect(status().isAccepted())
                .andExpect(header().string(NotificationController.REPLAYED_HEADER, "true"))
                .andReturn();

        assertThat(idOf(second)).isEqualTo(idOf(first));
        assertThat(second.getResponse().getHeader("Location")).isEqualTo(first.getResponse().getHeader("Location"));
        assertThat(countFor(userId, "notifications", "user_id")).isEqualTo(1);
        assertThat(countFor(userId, "outbox", "message_key")).isEqualTo(1);
    }

    @Test
    void reusingIdempotencyKeyWithDifferentBodyIsRejected() throws Exception {
        String userId = uniqueUserId();
        postWithKey(emailRequest(userId, "First"), "key-1").andExpect(status().isAccepted());

        postWithKey(emailRequest(userId, "Second"), "key-1")
                .andExpect(status().isUnprocessableContent())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE));

        assertThat(countFor(userId, "notifications", "user_id")).isEqualTo(1);
    }

    @Test
    void idempotencyKeysAreScopedPerUser() throws Exception {
        String alice = uniqueUserId();
        String bob = uniqueUserId();

        UUID aliceId = idOf(postWithKey(emailRequest(alice, "Hi"), "shared-key").andReturn());
        UUID bobId = idOf(postWithKey(emailRequest(bob, "Hi"), "shared-key").andReturn());

        assertThat(bobId).isNotEqualTo(aliceId);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "has space", "tab\tkey", "café"})
    void invalidIdempotencyKeyIsRejected(String key) throws Exception {
        String userId = uniqueUserId();
        postWithKey(emailRequest(userId, "Hi"), key)
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE));

        assertThat(countFor(userId, "notifications", "user_id")).isZero();
    }

    @Test
    void idempotencyKeyLongerThan255CharactersIsRejected() throws Exception {
        postWithKey(emailRequest(uniqueUserId(), "Hi"), "k".repeat(256)).andExpect(status().isBadRequest());
    }

    @Test
    void concurrentRequestsWithTheSameKeyCreateExactlyOneNotification() throws Exception {
        String userId = uniqueUserId();
        CreateNotificationRequest request = new CreateNotificationRequest(userId, Channel.EMAIL,
                "alice@example.com", "Concurrent", "Body");
        IdempotencyKey key = new IdempotencyKey("concurrent-key");
        int clients = 10;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<CreateResult>> results = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(clients)) {
            for (int i = 0; i < clients; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return service.create(request, key);
                }));
            }
            start.countDown();

            List<CreateResult> created = new ArrayList<>();
            for (Future<CreateResult> result : results) {
                created.add(result.get());
            }
            Set<UUID> ids = created.stream().map(r -> r.notification().id()).collect(Collectors.toSet());
            assertThat(ids).hasSize(1);
            assertThat(created).filteredOn(r -> !r.replayed()).hasSize(1);
        }
        assertThat(countFor(userId, "notifications", "user_id")).isEqualTo(1);
    }

    @Test
    void countsCreatedAndReplayedRequestsByChannelAndPriority() throws Exception {
        double created = requests("EMAIL", "created");
        double replayed = requests("EMAIL", "replayed");
        String body = emailRequest(uniqueUserId(), "Counted");
        String key = "counted-" + UUID.randomUUID();

        postWithKey(body, key).andExpect(status().isAccepted());
        postWithKey(body, key).andExpect(status().isAccepted());

        assertThat(requests("EMAIL", "created")).isEqualTo(created + 1);
        assertThat(requests("EMAIL", "replayed")).isEqualTo(replayed + 1);
    }

    @Test
    void logsTheAcceptedNotificationAsJsonWithItsId(CapturedOutput output) throws Exception {
        UUID id = createEmail(uniqueUserId(), "Logged");

        JsonNode line = jsonMapper.readTree(output.getOut().lines()
                .filter(l -> l.contains("Accepted EMAIL notification " + id))
                .findFirst()
                .orElseThrow());
        assertThat(line.path("notificationId").asString()).isEqualTo(id.toString());
        assertThat(line.path("service").path("name").asString()).isEqualTo("api");
    }

    private double requests(String channel, String result) {
        Counter counter = registry.find(NotificationService.REQUESTS)
                .tags("channel", channel, "priority", "NORMAL", "result", result).counter();
        return counter == null ? 0 : counter.count();
    }

    private ResultActions postWithKey(String body, String key)
            throws Exception {
        return mockMvc.perform(post("/notifications")
                .contentType(MediaType.APPLICATION_JSON)
                .header(IdempotencyKey.HEADER, key)
                .content(body));
    }

    private UUID idOf(MvcResult result) throws Exception {
        return UUID.fromString(jsonMapper.readTree(result.getResponse().getContentAsString()).get("id").asString());
    }

    /** Table and column come from the test itself, never from input. */
    private long countFor(String userId, String table, String column) {
        return jdbc.sql("SELECT count(*) FROM " + table + " WHERE " + column + " = :userId")
                .param("userId", userId)
                .query(Long.class)
                .single();
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

    private String smsRequest(String userId, String body) {
        return jsonMapper.writeValueAsString(Map.of(
                "userId", userId,
                "channel", "SMS",
                "recipient", "+6591234567",
                "body", body));
    }

    private ConsumerRecord<String, String> awaitRecordFor(UUID notificationId) {
        return awaitRecordFor(notificationId, EMAIL_TOPIC);
    }

    private ConsumerRecord<String, String> awaitRecordFor(UUID notificationId, String topic) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
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
