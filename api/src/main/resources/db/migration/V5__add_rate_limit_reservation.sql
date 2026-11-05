-- Set by the worker once a notification holds a rate-limit slot, so a notification deferred to
-- its slot, or a retried send, does not take a second one.
ALTER TABLE notifications ADD COLUMN rate_limit_reserved BOOLEAN NOT NULL DEFAULT FALSE;
