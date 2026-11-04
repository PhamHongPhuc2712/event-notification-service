package com.eventnotification.worker.delivery;

/** A delivery failure that retrying cannot fix. The notification is marked FAILED. */
public class PermanentDeliveryException extends RuntimeException {

    private final String failureCode;

    /** @param failureCode a public code from {@link FailureCodes}; the raw error stays in the logs */
    public PermanentDeliveryException(String failureCode, String message, Throwable cause) {
        super(message, cause);
        this.failureCode = failureCode;
    }

    public String failureCode() {
        return failureCode;
    }
}
