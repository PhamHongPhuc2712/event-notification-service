package com.eventnotification.worker.delivery;

/** The Kafka record can never be processed, so it goes straight to the dead-letter topic. */
public class MalformedMessageException extends RuntimeException {

    public MalformedMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
