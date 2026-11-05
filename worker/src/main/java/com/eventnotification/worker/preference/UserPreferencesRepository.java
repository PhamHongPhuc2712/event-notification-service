package com.eventnotification.worker.preference;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Set;

@Repository
public class UserPreferencesRepository {

    private final JdbcClient jdbc;

    public UserPreferencesRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Empty if the user never set preferences, which means every channel, any time. */
    public Optional<UserPreferences> find(String userId) {
        return jdbc.sql("SELECT * FROM user_preferences WHERE user_id = :userId")
                .param("userId", userId)
                .query(UserPreferencesRepository::map)
                .optional();
    }

    private static UserPreferences map(ResultSet rs, int rowNum) throws SQLException {
        Set<String> channels = Set.of((String[]) rs.getArray("allowed_channels").getArray());
        Time start = rs.getTime("quiet_start");
        Optional<QuietHours> quiet = start == null ? Optional.empty()
                : Optional.of(new QuietHours(start.toLocalTime(), rs.getTime("quiet_end").toLocalTime(),
                        ZoneId.of(rs.getString("time_zone"))));
        return new UserPreferences(channels, quiet);
    }
}
