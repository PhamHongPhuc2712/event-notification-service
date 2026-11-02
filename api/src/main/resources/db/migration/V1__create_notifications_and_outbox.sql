-- Notifications accepted by the API and their delivery status.
CREATE TABLE notifications (
    id             UUID         PRIMARY KEY,
    user_id        VARCHAR(64)  NOT NULL,
    channel        VARCHAR(16)  NOT NULL CHECK (channel IN ('EMAIL', 'SMS', 'WEBHOOK')),
    recipient      VARCHAR(320) NOT NULL,
    subject        VARCHAR(200),
    body           TEXT         NOT NULL,
    status         VARCHAR(16)  NOT NULL CHECK (status IN ('QUEUED', 'SENT', 'FAILED')),
    failure_reason TEXT,
    created_at     TIMESTAMPTZ  NOT NULL,
    updated_at     TIMESTAMPTZ  NOT NULL,
    sent_at        TIMESTAMPTZ
);

CREATE INDEX idx_notifications_user_created ON notifications (user_id, created_at DESC);

-- Transactional outbox: written in the same transaction as the notification,
-- then published to Kafka by the outbox publisher.
CREATE TABLE outbox (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id UUID         NOT NULL,
    topic        VARCHAR(128) NOT NULL,
    message_key  VARCHAR(128) NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ
);

-- Small partial index: only unpublished rows, scanned in insertion order.
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
