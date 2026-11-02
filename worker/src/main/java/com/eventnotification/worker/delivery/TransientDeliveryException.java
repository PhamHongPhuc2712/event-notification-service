package com.eventnotification.worker.delivery;

/** A delivery failure that may succeed if retried, such as the mail server being unreachable. */
public class TransientDeliveryException extends RuntimeException {

    public TransientDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
