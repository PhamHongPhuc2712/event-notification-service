-- A worker inserts a row here before sending a notification. The primary key makes the
-- claim atomic: only one worker can hold a live claim. A claim older than the lease belongs
-- to a worker that probably crashed, so another worker may take it over.
CREATE TABLE delivery_claims (
    notification_id UUID        PRIMARY KEY REFERENCES notifications (id) ON DELETE CASCADE,
    claimed_by      VARCHAR(64) NOT NULL,
    claimed_at      TIMESTAMPTZ NOT NULL
);
