package com.eventnotification.api.preference;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * A daily window, in the user's time zone, during which nothing is sent. When {@code start}
 * is after {@code end} the window crosses midnight, such as 22:00 to 07:00.
 */
public record QuietHours(@NotNull LocalTime start, @NotNull LocalTime end, @NotBlank String timeZone) {

    @JsonIgnore
    @AssertTrue(message = "start and end must differ")
    public boolean isStartDifferentFromEnd() {
        return start == null || !start.equals(end);
    }

    @JsonIgnore
    @AssertTrue(message = "timeZone must be an IANA zone ID such as Asia/Singapore")
    public boolean isTimeZoneValid() {
        if (timeZone == null) {
            return true;
        }
        try {
            ZoneId.of(timeZone);
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }
}
