package com.eventnotification.mocksms.sms;

import com.eventnotification.mocksms.faults.FaultInjector.Fault;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Accepted messages, kept in memory. Like a real provider, it accepts each idempotency key once. */
@Component
public class SmsStore {

    /** Result of a send: the stored message, and whether an earlier request already sent it. */
    public record Accepted(SmsMessage message, boolean replayed) {
    }

    private final Map<String, SmsMessage> byKey = new ConcurrentHashMap<>();
    private final LongAdder requests = new LongAdder();
    private final LongAdder duplicateRequests = new LongAdder();
    private final Map<Fault, LongAdder> faults = new ConcurrentHashMap<>();
    private final Clock clock;

    public SmsStore(Clock clock) {
        this.clock = clock;
    }

    public void recordRequest() {
        requests.increment();
    }

    public void recordFault(Fault fault) {
        faults.computeIfAbsent(fault, f -> new LongAdder()).increment();
    }

    /**
     * Stores the message unless the key was seen before.
     *
     * @throws IdempotencyKeyReusedException if the key was used for a different message
     */
    public Accepted accept(String idempotencyKey, SendSmsRequest request) {
        SmsMessage candidate = new SmsMessage(UUID.randomUUID(), idempotencyKey, request.to(), request.body(),
                clock.instant());
        SmsMessage existing = byKey.putIfAbsent(idempotencyKey, candidate);
        if (existing == null) {
            return new Accepted(candidate, false);
        }
        if (!existing.to().equals(request.to()) || !existing.body().equals(request.body())) {
            throw new IdempotencyKeyReusedException();
        }
        duplicateRequests.increment();
        return new Accepted(existing, true);
    }

    public List<SmsMessage> messages() {
        return byKey.values().stream().sorted(Comparator.comparing(SmsMessage::acceptedAt)).toList();
    }

    public SmsStats stats() {
        return new SmsStats(requests.sum(), byKey.size(), duplicateRequests.sum(), faultCount(Fault.FAILURE),
                faultCount(Fault.TIMEOUT), faultCount(Fault.SLOW));
    }

    public void reset() {
        byKey.clear();
        requests.reset();
        duplicateRequests.reset();
        faults.clear();
    }

    private long faultCount(Fault fault) {
        LongAdder count = faults.get(fault);
        return count == null ? 0 : count.sum();
    }
}
