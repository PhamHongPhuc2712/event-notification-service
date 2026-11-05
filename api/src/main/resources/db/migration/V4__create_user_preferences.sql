-- Per-user delivery preferences. A user without a row allows every channel and has no quiet hours.
CREATE TABLE user_preferences (
    user_id          VARCHAR(64) PRIMARY KEY,
    allowed_channels TEXT[]      NOT NULL
        CHECK (allowed_channels <@ ARRAY['EMAIL', 'SMS', 'WEBHOOK']::TEXT[]),
    -- Quiet hours are local times in time_zone. start > end means the window crosses midnight.
    quiet_start      TIME,
    quiet_end        TIME,
    time_zone        VARCHAR(64),
    updated_at       TIMESTAMPTZ NOT NULL,
    CHECK ((quiet_start IS NULL) = (quiet_end IS NULL) AND (quiet_start IS NULL) = (time_zone IS NULL)),
    CHECK (quiet_start IS NULL OR quiet_start <> quiet_end)
);

-- DEFERRED: held back by quiet hours until deliver_after. SUPPRESSED: the user turned the channel off.
ALTER TABLE notifications DROP CONSTRAINT notifications_status_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_status_check
    CHECK (status IN ('QUEUED', 'DEFERRED', 'SENT', 'FAILED', 'SUPPRESSED'));
ALTER TABLE notifications ADD COLUMN deliver_after TIMESTAMPTZ;

CREATE INDEX idx_notifications_deferred ON notifications (deliver_after) WHERE status = 'DEFERRED';
