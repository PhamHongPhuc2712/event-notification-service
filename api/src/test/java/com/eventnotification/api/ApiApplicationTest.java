package com.eventnotification.api;

import com.eventnotification.api.metrics.BacklogMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Spring Boot disables metrics export in tests by default; turn Prometheus on to test the endpoint.
@SpringBootTest(properties = "management.prometheus.metrics.export.enabled=true")
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class ApiApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private BacklogMetrics backlogMetrics;

    @Test
    void healthEndpointReportsUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void prometheusEndpointIsExposed() throws Exception {
        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk());
    }

    @Test
    void backlogGaugesAreReadFromTheDatabaseAndExported() throws Exception {
        backlogMetrics.refresh();

        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("notification_outbox_backlog{")))
                .andExpect(content().string(containsString("notification_outbox_oldest_age_seconds{")))
                .andExpect(content().string(containsString("notification_deferred_backlog{")))
                .andExpect(content().string(not(containsString("notification_outbox_backlog{application=\"api\"} NaN"))));
    }
}
