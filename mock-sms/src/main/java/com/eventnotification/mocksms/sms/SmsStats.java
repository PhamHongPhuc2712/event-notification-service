package com.eventnotification.mocksms.sms;

/**
 * Counters since the last reset.
 *
 * @param requests           every send request, including rejected ones
 * @param accepted           distinct messages accepted, one per idempotency key
 * @param duplicateRequests  requests that repeated an accepted key: retries the provider absorbed.
 *                           Without the key, each would have been a second SMS to the user.
 */
public record SmsStats(long requests, long accepted, long duplicateRequests, long injectedFailures,
                       long injectedTimeouts, long injectedSlowResponses) {
}
