-- Idempotency keys sent by clients, scoped per user. The key row is inserted in the same
-- transaction as the notification, so a retried request finds the notification it created.
-- The foreign key is deferred because the key row is inserted first: that insert is what makes
-- a concurrent duplicate wait until the first request commits or rolls back.
CREATE TABLE idempotency_keys (
    user_id         VARCHAR(64)  NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    request_hash    CHAR(64)     NOT NULL,
    notification_id UUID         NOT NULL REFERENCES notifications (id) DEFERRABLE INITIALLY DEFERRED,
    created_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (user_id, idempotency_key)
);
