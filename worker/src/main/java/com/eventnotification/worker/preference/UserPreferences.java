package com.eventnotification.worker.preference;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * A user's delivery preferences, written by the api module.
 *
 * @param allowedChannels channel names the user accepts, such as EMAIL
 */
public record UserPreferences(Set<String> allowedChannels, Optional<QuietHours> quietHours) {

    public boolean allows(String channel) {
        return allowedChannels.contains(channel);
    }

    /** When the current quiet hours end, or empty if it is not quiet now. */
    public Optional<Instant> quietUntil(Instant now) {
        return quietHours.filter(q -> q.isQuietAt(now)).map(q -> q.nextEndAfter(now));
    }
}
