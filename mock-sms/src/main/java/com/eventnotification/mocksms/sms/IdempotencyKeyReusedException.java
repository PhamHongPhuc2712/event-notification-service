package com.eventnotification.mocksms.sms;

public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException() {
        super("Idempotency-Key was already used for a different message");
    }
}
