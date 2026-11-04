package com.eventnotification.api.idempotency;

public class InvalidIdempotencyKeyException extends RuntimeException {

    public InvalidIdempotencyKeyException() {
        super(IdempotencyKey.HEADER + " must be 1 to 255 printable ASCII characters without spaces");
    }
}
