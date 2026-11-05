package com.eventnotification.worker.webhook;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

/**
 * Signs webhook requests the way the Standard Webhooks spec (standardwebhooks.com) describes,
 * so receivers can verify them with an off-the-shelf library.
 *
 * <p>The signature is {@code v1,<base64 HMAC-SHA256 of "id.timestamp.body">}. Signing the ID
 * and timestamp along with the body stops an attacker from replaying a captured request later
 * or under another ID: receivers reject timestamps more than a few minutes old.
 */
public final class WebhookSigner {

    static final String SECRET_PREFIX = "whsec_";
    private static final String ALGORITHM = "HmacSHA256";
    private static final int MIN_KEY_BYTES = 24;
    private static final int MAX_KEY_BYTES = 64;

    private final SecretKeySpec key;

    /** @param secret {@code whsec_} followed by the base64 encoding of 24 to 64 random bytes */
    public WebhookSigner(String secret) {
        if (secret == null || !secret.startsWith(SECRET_PREFIX)) {
            throw new IllegalArgumentException("Webhook signing secret must start with " + SECRET_PREFIX);
        }
        byte[] keyBytes;
        try {
            keyBytes = Base64.getDecoder().decode(secret.substring(SECRET_PREFIX.length()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Webhook signing secret is not valid base64 after " + SECRET_PREFIX);
        }
        if (keyBytes.length < MIN_KEY_BYTES || keyBytes.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("Webhook signing secret must decode to " + MIN_KEY_BYTES + " to "
                    + MAX_KEY_BYTES + " bytes");
        }
        this.key = new SecretKeySpec(keyBytes, ALGORITHM);
    }

    public String sign(String messageId, long timestampSeconds, String body) {
        String signed = messageId + "." + timestampSeconds + "." + body;
        try {
            // Mac is not thread-safe, and creating one is cheap next to an HTTP request.
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(key);
            return "v1," + Base64.getEncoder().encodeToString(mac.doFinal(signed.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is required by every Java runtime", e);
        }
    }
}
