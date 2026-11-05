package com.eventnotification.worker.preference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class QuietHoursTest {

    private static final ZoneId SINGAPORE = ZoneId.of("Asia/Singapore");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    private final QuietHours overnight = new QuietHours(LocalTime.of(22, 0), LocalTime.of(7, 0), SINGAPORE);
    private final QuietHours afternoon = new QuietHours(LocalTime.of(13, 0), LocalTime.of(14, 30), SINGAPORE);

    @ParameterizedTest
    @CsvSource({
            "21:59, false",
            "22:00, true",  // start is inclusive
            "23:30, true",
            "00:00, true",
            "06:59, true",
            "07:00, false", // end is exclusive
            "12:00, false"
    })
    void overnightWindowCrossesMidnight(String localTime, boolean quiet) {
        assertThat(overnight.isQuietAt(at("2026-11-05", localTime, SINGAPORE))).isEqualTo(quiet);
    }

    @ParameterizedTest
    @CsvSource({"12:59, false", "13:00, true", "14:29, true", "14:30, false"})
    void sameDayWindow(String localTime, boolean quiet) {
        assertThat(afternoon.isQuietAt(at("2026-11-05", localTime, SINGAPORE))).isEqualTo(quiet);
    }

    @Test
    void usesTheUsersTimeZoneNotUtc() {
        // 15:00 UTC is 23:00 in Singapore.
        assertThat(overnight.isQuietAt(Instant.parse("2026-11-05T15:00:00Z"))).isTrue();
        assertThat(overnight.isQuietAt(Instant.parse("2026-11-05T03:00:00Z"))).isFalse();
    }

    @Test
    void nextEndBeforeMidnightIsTomorrowMorning() {
        Instant late = at("2026-11-05", "23:00", SINGAPORE);
        assertThat(overnight.nextEndAfter(late)).isEqualTo(at("2026-11-06", "07:00", SINGAPORE));
    }

    @Test
    void nextEndAfterMidnightIsThisMorning() {
        Instant early = at("2026-11-06", "02:00", SINGAPORE);
        assertThat(overnight.nextEndAfter(early)).isEqualTo(at("2026-11-06", "07:00", SINGAPORE));
    }

    @Test
    void endThatFallsInADaylightSavingGapMovesPastTheGap() {
        // New York skips 02:00-03:00 on 2027-03-14.
        QuietHours untilTwoThirty = new QuietHours(LocalTime.of(23, 0), LocalTime.of(2, 30), NEW_YORK);
        Instant beforeGap = at("2027-03-14", "01:00", NEW_YORK);

        assertThat(untilTwoThirty.nextEndAfter(beforeGap)).isEqualTo(Instant.parse("2027-03-14T07:30:00Z"));
        assertThat(untilTwoThirty.isQuietAt(untilTwoThirty.nextEndAfter(beforeGap))).isFalse();
    }

    @Test
    void notificationReleasedAtTheEndIsNoLongerQuiet() {
        Instant late = at("2026-11-05", "23:00", SINGAPORE);
        assertThat(overnight.isQuietAt(overnight.nextEndAfter(late))).isFalse();
    }

    private static Instant at(String date, String time, ZoneId zone) {
        return ZonedDateTime.of(LocalDate.parse(date), LocalTime.parse(time), zone).toInstant();
    }
}
