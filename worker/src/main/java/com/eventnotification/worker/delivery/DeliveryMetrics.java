package com.eventnotification.worker.delivery;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

/**
 * Delivery metrics, exported to Prometheus as:
 * <ul>
 *   <li>{@code notification_deliveries_total{channel, priority, outcome}}: one per delivery
 *       attempt. The outcome is a {@link DeliveryOutcome} in lower case, {@code retry} when a
 *       send failed in a way a retry may fix, or {@code error} for anything else that sends the
 *       record to a retry topic, such as the database being down.</li>
 *   <li>{@code notification_send_seconds{channel, result}}: how long the provider took to
 *       answer, by {@code success}, {@code permanent_failure}, {@code transient_failure} or
 *       {@code error}.</li>
 *   <li>{@code notification_delivery_latency_seconds{channel, priority}}: from the moment the
 *       API accepted the notification until it was sent, including time spent in retries and
 *       quiet hours.</li>
 * </ul>
 * The two timers publish histograms, so Grafana can show percentiles across all workers.
 */
@Component
public class DeliveryMetrics {

    public static final String DELIVERIES = "notification.deliveries";
    public static final String SEND = "notification.send";
    public static final String LATENCY = "notification.delivery.latency";

    static final String RETRY = "retry";
    static final String ERROR = "error";
    static final String SUCCESS = "success";
    static final String PERMANENT_FAILURE = "permanent_failure";
    static final String TRANSIENT_FAILURE = "transient_failure";

    /** The project's latency target, so dashboards can show the share of notifications that meet it. */
    private static final Duration LATENCY_TARGET = Duration.ofMillis(200);

    private final MeterRegistry registry;

    public DeliveryMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    void outcome(NotificationMessage message, DeliveryOutcome outcome) {
        outcome(message, outcome.name().toLowerCase(Locale.ROOT));
    }

    void outcome(NotificationMessage message, String outcome) {
        Counter.builder(DELIVERIES)
                .description("Delivery attempts by outcome")
                .tag("channel", channel(message))
                .tag("priority", priority(message))
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }

    Timer.Sample startSend() {
        return Timer.start(registry);
    }

    void stopSend(Timer.Sample sample, NotificationMessage message, String result) {
        sample.stop(Timer.builder(SEND)
                .description("Time for the provider to answer one send")
                .tag("channel", channel(message))
                .tag("result", result)
                .publishPercentileHistogram()
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(registry));
    }

    /** Records the end-to-end latency, if the message says when it was created. */
    void sent(NotificationMessage message, Instant sentAt) {
        if (message.createdAt() == null) {
            return;
        }
        Duration latency = Duration.between(message.createdAt(), sentAt);
        Timer.builder(LATENCY)
                .description("Time from acceptance by the API until sent")
                .tag("channel", channel(message))
                .tag("priority", priority(message))
                .publishPercentileHistogram()
                .serviceLevelObjectives(LATENCY_TARGET)
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(registry)
                .record(latency.isNegative() ? Duration.ZERO : latency);
    }

    private static String channel(NotificationMessage message) {
        return message.channel() == null ? "UNKNOWN" : message.channel();
    }

    private static String priority(NotificationMessage message) {
        return message.isHighPriority() ? NotificationMessage.HIGH : "NORMAL";
    }
}
