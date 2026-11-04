package com.eventnotification.api.idempotency;

import java.util.regex.Pattern;

/** A client-chosen key that makes retrying a request safe. 1 to 255 printable ASCII characters. */
public record IdempotencyKey(String value) {

    public static final String HEADER = "Idempotency-Key";

    private static final Pattern VALID = Pattern.compile("[\\x21-\\x7E]{1,255}");

    public IdempotencyKey {
        if (value == null || !VALID.matcher(value).matches()) {
            throw new InvalidIdempotencyKeyException();
        }
    }
}
