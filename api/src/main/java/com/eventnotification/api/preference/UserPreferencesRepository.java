package com.eventnotification.api.preference;

import com.eventnotification.api.notification.Channel;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
public class UserPreferencesRepository {

    private final JdbcClient jdbc;

    public UserPreferencesRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<UserPreferences> find(String userId) {
        return jdbc.sql("SELECT * FROM user_preferences WHERE user_id = :userId")
                .param("userId", userId)
                .query(UserPreferencesRepository::map)
                .optional();
    }

    /** Creates or replaces the user's preferences. */
    public void save(String userId, UserPreferences preferences, Instant now) {
        QuietHours quiet = preferences.quietHours();
        jdbc.sql("""
                        INSERT INTO user_preferences
                            (user_id, allowed_channels, quiet_start, quiet_end, time_zone, updated_at)
                        VALUES (:userId, CAST(:channels AS TEXT[]), :start, :end, :zone, :now)
                        ON CONFLICT (user_id) DO UPDATE SET
                            allowed_channels = EXCLUDED.allowed_channels,
                            quiet_start = EXCLUDED.quiet_start,
                            quiet_end = EXCLUDED.quiet_end,
                            time_zone = EXCLUDED.time_zone,
                            updated_at = EXCLUDED.updated_at
                        """)
                .param("userId", userId)
                .param("channels", toArrayLiteral(preferences.allowedChannels()))
                .param("start", quiet == null ? null : Time.valueOf(quiet.start()))
                .param("end", quiet == null ? null : Time.valueOf(quiet.end()))
                .param("zone", quiet == null ? null : quiet.timeZone())
                .param("now", Timestamp.from(now))
                .update();
    }

    /** A Postgres array literal such as {EMAIL,SMS}. Enum names need no quoting. */
    private static String toArrayLiteral(Set<Channel> channels) {
        return channels.stream().map(Channel::name).sorted().collect(Collectors.joining(",", "{", "}"));
    }

    private static UserPreferences map(ResultSet rs, int rowNum) throws SQLException {
        Set<Channel> channels = EnumSet.noneOf(Channel.class);
        Arrays.stream((String[]) rs.getArray("allowed_channels").getArray())
                .map(Channel::valueOf)
                .forEach(channels::add);
        Time start = rs.getTime("quiet_start");
        QuietHours quiet = start == null ? null
                : new QuietHours(start.toLocalTime(), rs.getTime("quiet_end").toLocalTime(), rs.getString("time_zone"));
        return new UserPreferences(channels, quiet);
    }
}
