package com.eventnotification.worker;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Spring Boot disables metrics export in tests by default; turn Prometheus on to test the endpoint.
@SpringBootTest(properties = "management.prometheus.metrics.export.enabled=true")
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class WorkerApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

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
    void retryAndDeadLetterTopicsUseFewerConsumersThanTheMainTopics() {
        // Every retry topic inherited the main concurrency: about 170 consumers per worker, most
        // of them idle, which loaded the broker and every proxy between worker and broker.
        assertThat(listeners.getListenerContainers())
                .filteredOn(c -> c.getListenerId().matches(".*-(retry-\\d+|dlt)"))
                .isNotEmpty()
                .allSatisfy(c -> assertThat(concurrency(c)).isEqualTo(TestWorkerProperties.RETRY_CONCURRENCY));
        assertThat(concurrency(listeners.getListenerContainer("sms-worker"))).isEqualTo(6);
    }

    private static int concurrency(MessageListenerContainer container) {
        return ((ConcurrentMessageListenerContainer<?, ?>) container).getConcurrency();
    }
}
