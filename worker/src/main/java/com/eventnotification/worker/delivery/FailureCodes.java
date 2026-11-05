package com.eventnotification.worker.delivery;

/** Failure reasons shown to API clients. Raw provider errors stay in the logs, because they reveal internal hosts. */
public final class FailureCodes {

    /** The provider refused the recipient, such as an unknown mailbox or unreachable number. */
    public static final String RECIPIENT_REJECTED = "RECIPIENT_REJECTED";
    /** The provider refused the request for another reason that retrying cannot fix. */
    public static final String REJECTED_BY_PROVIDER = "REJECTED_BY_PROVIDER";
    /** Every retry failed and the message went to the dead-letter topic. It can be replayed. */
    public static final String RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED";
    /** Not a failure: the user turned this channel off, so the notification was SUPPRESSED. */
    public static final String CHANNEL_DISABLED = "CHANNEL_DISABLED";

    private FailureCodes() {
    }
}
