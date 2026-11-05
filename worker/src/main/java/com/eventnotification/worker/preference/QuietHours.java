package com.eventnotification.worker.preference;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A daily window in the user's time zone during which nothing is sent. The start is inclusive
 * and the end exclusive; a start after the end crosses midnight, such as 22:00 to 07:00.
 */
public record QuietHours(LocalTime start, LocalTime end, ZoneId zone) {

    public boolean isQuietAt(Instant instant) {
        LocalTime local = instant.atZone(zone).toLocalTime();
        if (start.isBefore(end)) {
            return !local.isBefore(start) && local.isBefore(end);
        }
        return !local.isBefore(start) || local.isBefore(end);
    }

    /**
     * The first instant after {@code instant} at which the window ends. If the end time does not
     * exist that day because clocks go forward, it is the first instant after the gap.
     */
    public Instant nextEndAfter(Instant instant) {
        ZonedDateTime now = instant.atZone(zone);
        LocalDate day = now.toLocalDate();
        ZonedDateTime end = ZonedDateTime.of(day, this.end, zone);
        if (!end.isAfter(now)) {
            end = ZonedDateTime.of(day.plusDays(1), this.end, zone);
        }
        return end.toInstant();
    }
}
