package com.eventnotification.api.preference;

import com.eventnotification.api.notification.Channel;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.EnumSet;
import java.util.Set;

/**
 * How a user wants to be notified.
 *
 * @param allowedChannels channels the user accepts; an empty set turns everything off
 * @param quietHours      when nothing is sent, or null for no quiet hours
 */
public record UserPreferences(@NotNull Set<@NotNull Channel> allowedChannels, @Valid QuietHours quietHours) {

    /** For a user who never set preferences: every channel, any time. */
    public static UserPreferences defaults() {
        return new UserPreferences(EnumSet.allOf(Channel.class), null);
    }
}
