package com.eventnotification.api.idempotency;

/** The key was already used for a request with a different body. */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException() {
        super("This " + IdempotencyKey.HEADER + " was already used with a different request body");
    }
}
