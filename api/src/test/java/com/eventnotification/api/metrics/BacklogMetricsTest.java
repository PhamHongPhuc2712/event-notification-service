package com.eventnotification.api.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BacklogMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final BacklogRepository repository = mock(BacklogRepository.class);
    private final BacklogMetrics metrics = new BacklogMetrics(repository, registry);

    @Test
    void exposesTheLatestBacklogAsGauges() {
        when(repository.read()).thenReturn(new BacklogRepository.Backlog(3, Duration.ofMillis(12_500), 5));

        metrics.refresh();

        assertThat(gauge(BacklogMetrics.OUTBOX_BACKLOG)).isEqualTo(3.0);
        assertThat(gauge(BacklogMetrics.OUTBOX_OLDEST_AGE)).isEqualTo(12.5);
        assertThat(gauge(BacklogMetrics.DEFERRED_BACKLOG)).isEqualTo(5.0);
    }

    @Test
    void reportsNothingRatherThanAStaleValueWhenTheDatabaseIsDown() {
        when(repository.read())
                .thenReturn(new BacklogRepository.Backlog(3, Duration.ZERO, 5))
                .thenThrow(new DataAccessResourceFailureException("connection refused"));

        metrics.refresh();
        metrics.refresh();

        assertThat(gauge(BacklogMetrics.OUTBOX_BACKLOG)).isNaN();
        assertThat(gauge(BacklogMetrics.OUTBOX_OLDEST_AGE)).isNaN();
        assertThat(gauge(BacklogMetrics.DEFERRED_BACKLOG)).isNaN();
    }

    @Test
    void startsUnknownUntilTheFirstRefresh() {
        assertThat(gauge(BacklogMetrics.OUTBOX_BACKLOG)).isNaN();
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }
}
