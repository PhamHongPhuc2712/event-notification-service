package com.eventnotification.mocksms.faults;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/** Picks a fault for each request. The settings can be swapped at runtime. */
@Component
public class FaultInjector {

    public enum Fault {
        NONE,
        FAILURE,
        TIMEOUT,
        SLOW
    }

    private final AtomicReference<FaultSettings> settings;

    public FaultInjector(FaultSettings initial) {
        this.settings = new AtomicReference<>(initial);
    }

    public FaultSettings settings() {
        return settings.get();
    }

    public void update(FaultSettings newSettings) {
        settings.set(newSettings);
    }

    public Fault next() {
        FaultSettings s = settings.get();
        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < s.failureRate()) {
            return Fault.FAILURE;
        }
        if (roll < s.failureRate() + s.timeoutRate()) {
            return Fault.TIMEOUT;
        }
        if (roll < s.failureRate() + s.timeoutRate() + s.slowRate()) {
            return Fault.SLOW;
        }
        return Fault.NONE;
    }
}
