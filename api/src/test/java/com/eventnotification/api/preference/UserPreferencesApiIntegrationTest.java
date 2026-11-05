package com.eventnotification.api.preference;

import com.eventnotification.api.TestcontainersConfiguration;
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
import org.springframework.test.web.servlet.ResultActions;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class UserPreferencesApiIntegrationTest {

    private static final String QUIET_OVERNIGHT = """
            {"allowedChannels":["EMAIL"],
             "quietHours":{"start":"22:00","end":"07:00","timeZone":"Asia/Singapore"}}""";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcClient jdbc;

    @Test
    void userWithoutPreferencesGetsTheDefaults() throws Exception {
        mockMvc.perform(get("/users/{userId}/preferences", uniqueUserId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowedChannels", containsInAnyOrder("EMAIL", "SMS")))
                .andExpect(jsonPath("$.quietHours").value(nullValue()));
    }

    @Test
    void savedPreferencesAreReturned() throws Exception {
        String userId = uniqueUserId();

        putPreferences(userId, QUIET_OVERNIGHT).andExpect(status().isOk());

        mockMvc.perform(get("/users/{userId}/preferences", userId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.allowedChannels", containsInAnyOrder("EMAIL")))
                .andExpect(jsonPath("$.quietHours.start").value("22:00:00"))
                .andExpect(jsonPath("$.quietHours.end").value("07:00:00"))
                .andExpect(jsonPath("$.quietHours.timeZone").value("Asia/Singapore"));
    }

    @Test
    void putReplacesThePreviousPreferences() throws Exception {
        String userId = uniqueUserId();
        putPreferences(userId, QUIET_OVERNIGHT).andExpect(status().isOk());

        putPreferences(userId, """
                {"allowedChannels":[]}""").andExpect(status().isOk());

        mockMvc.perform(get("/users/{userId}/preferences", userId))
                .andExpect(jsonPath("$.allowedChannels").isEmpty())
                .andExpect(jsonPath("$.quietHours").value(nullValue()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // no channels field
            """
            {"quietHours":null}""",
            // unknown channel
            """
            {"allowedChannels":["PIGEON"]}""",
            // null channel
            """
            {"allowedChannels":[null]}""",
            // empty window
            """
            {"allowedChannels":["EMAIL"],"quietHours":{"start":"22:00","end":"22:00","timeZone":"UTC"}}""",
            // unknown time zone
            """
            {"allowedChannels":["EMAIL"],"quietHours":{"start":"22:00","end":"07:00","timeZone":"Mars/Olympus"}}""",
            // missing end
            """
            {"allowedChannels":["EMAIL"],"quietHours":{"start":"22:00","timeZone":"UTC"}}""",
            // not a time
            """
            {"allowedChannels":["EMAIL"],"quietHours":{"start":"10pm","end":"07:00","timeZone":"UTC"}}"""
    })
    void invalidPreferencesAreRejected(String body) throws Exception {
        String userId = uniqueUserId();

        putPreferences(userId, body)
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE));

        assertThat(jdbc.sql("SELECT count(*) FROM user_preferences WHERE user_id = :id")
                .param("id", userId).query(Long.class).single()).isZero();
    }

    @Test
    void invalidUserIdIsRejected() throws Exception {
        putPreferences("u".repeat(65), QUIET_OVERNIGHT).andExpect(status().isBadRequest());
        mockMvc.perform(get("/users/{userId}/preferences", "bad id")).andExpect(status().isBadRequest());
    }

    @Test
    void deferredNotificationIsQueuedAndPublishedAgainOnceQuietHoursEnd() {
        UUID due = insertDeferred(Instant.now().minusSeconds(1));
        UUID notYet = insertDeferred(Instant.now().plus(Duration.ofHours(1)));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(statusOf(due)).isEqualTo("QUEUED");
            assertThat(jdbc.sql("SELECT published_at IS NOT NULL FROM outbox WHERE aggregate_id = :id")
                    .param("id", due).query(Boolean.class).single()).isTrue();
        });
        assertThat(statusOf(notYet)).isEqualTo("DEFERRED");
        assertThat(jdbc.sql("SELECT deliver_after IS NULL FROM notifications WHERE id = :id")
                .param("id", due).query(Boolean.class).single()).isTrue();
    }

    @Test
    void deferredNotificationShowsWhenItWillBeDelivered() throws Exception {
        Instant deliverAfter = Instant.parse("2099-01-01T07:00:00Z");
        UUID id = insertDeferred(deliverAfter);

        mockMvc.perform(get("/notifications/{id}", id))
                .andExpect(jsonPath("$.status").value("DEFERRED"))
                .andExpect(jsonPath("$.deliverAfter").value("2099-01-01T07:00:00Z"));
    }

    private ResultActions putPreferences(String userId, String body) throws Exception {
        return mockMvc.perform(put("/users/{userId}/preferences", userId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    private UUID insertDeferred(Instant deliverAfter) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.sql("""
                        INSERT INTO notifications (id, user_id, channel, recipient, subject, body, status,
                                                   created_at, updated_at, deliver_after)
                        VALUES (:id, :userId, 'EMAIL', 'a@example.com', 'Hi', 'Body', 'DEFERRED', :now, :now,
                                :deliverAfter)
                        """)
                .param("id", id)
                .param("userId", uniqueUserId())
                .param("now", now)
                .param("deliverAfter", Timestamp.from(deliverAfter))
                .update();
        return id;
    }

    private String statusOf(UUID id) {
        return jdbc.sql("SELECT status FROM notifications WHERE id = :id").param("id", id).query(String.class).single();
    }

    private static String uniqueUserId() {
        return "user-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
