-- HIGH notifications, such as password resets, use separate topics and skip quiet hours.
ALTER TABLE notifications ADD COLUMN priority VARCHAR(8) NOT NULL DEFAULT 'NORMAL'
    CHECK (priority IN ('NORMAL', 'HIGH'));
