package com.eventnotification.api.outbox;

import com.eventnotification.api.config.NotificationProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxCleanerTest {

    private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
    private static final Instant CUTOFF = NOW.minus(Duration.ofHours(1));

    private final OutboxRepository outbox = mock(OutboxRepository.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final OutboxCleaner cleaner = new OutboxCleaner(outbox,
            new NotificationProperties.Outbox.Cleanup(Duration.ofMinutes(1), Duration.ofHours(1), 1000),
            registry, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void deletesRowsPublishedBeforeTheRetentionWindow() {
        when(outbox.deletePublishedBefore(CUTOFF, 1000)).thenReturn(7);

        assertThat(cleaner.deletePublished()).isEqualTo(7);

        verify(outbox).deletePublishedBefore(CUTOFF, 1000);
        assertThat(registry.get(OutboxCleaner.DELETED).counter().count()).isEqualTo(7.0);
    }

    @Test
    void keepsDeletingWhileBatchesComeBackFull() {
        when(outbox.deletePublishedBefore(CUTOFF, 1000)).thenReturn(1000, 1000, 3);

        assertThat(cleaner.deletePublished()).isEqualTo(2003);

        verify(outbox, times(3)).deletePublishedBefore(CUTOFF, 1000);
        assertThat(registry.get(OutboxCleaner.DELETED).counter().count()).isEqualTo(2003.0);
    }

    @Test
    void doesNothingWhenNoRowsAreOldEnough() {
        when(outbox.deletePublishedBefore(any(), anyInt())).thenReturn(0);

        assertThat(cleaner.deletePublished()).isZero();

        verify(outbox, times(1)).deletePublishedBefore(CUTOFF, 1000);
    }

    @Test
    void scheduledRunSurvivesADatabaseFailureAndCountsIt() {
        when(outbox.deletePublishedBefore(any(), anyInt()))
                .thenThrow(new DataAccessResourceFailureException("connection refused"));

        cleaner.cleanUp();

        assertThat(registry.get(OutboxCleaner.FAILURES).counter().count()).isEqualTo(1.0);
        assertThat(registry.get(OutboxCleaner.DELETED).counter().count()).isZero();
    }
}
