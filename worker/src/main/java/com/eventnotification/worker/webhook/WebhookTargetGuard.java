package com.eventnotification.worker.webhook;

import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

/**
 * Stops webhooks from being aimed at the service's own network (server-side request forgery).
 * Without it, anyone who can create a notification could make the worker POST to
 * {@code http://169.254.169.254/} (cloud credentials), to the database host, or to an admin
 * port on localhost.
 *
 * <p>Only https is allowed, and every address the host name resolves to must be public. Local
 * development turns this off with {@code notifications.webhook.allow-private-targets}.
 *
 * <p>Not covered: DNS rebinding. The HTTP client resolves the name again when it connects, so a
 * name that answers with a public address here and a private one a moment later gets through.
 * Closing that gap means connecting to the address checked here, which needs a resolver hook the
 * JDK HTTP client does not offer; an egress proxy that applies the same rules is the usual fix.
 */
public class WebhookTargetGuard {

    private final boolean allowPrivateTargets;
    private final Resolver resolver;

    /** Resolves a host name; replaceable in tests. */
    @FunctionalInterface
    public interface Resolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    public WebhookTargetGuard(boolean allowPrivateTargets) {
        this(allowPrivateTargets, InetAddress::getAllByName);
    }

    WebhookTargetGuard(boolean allowPrivateTargets, Resolver resolver) {
        this.allowPrivateTargets = allowPrivateTargets;
        this.resolver = resolver;
    }

    /**
     * @return the URL to POST to
     * @throws PermanentDeliveryException if the URL or any address its host resolves to is not allowed
     * @throws TransientDeliveryException if the host name could not be resolved, which may pass
     */
    public URI check(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw notAllowed(url, "is not a valid URL");
        }
        if (uri.getHost() == null || uri.getRawUserInfo() != null) {
            throw notAllowed(url, "needs a host and no credentials");
        }
        if (allowPrivateTargets) {
            return uri;
        }
        if (!"https".equals(uri.getScheme())) {
            throw notAllowed(url, "must use https");
        }
        InetAddress[] addresses;
        try {
            addresses = resolver.resolve(uri.getHost());
        } catch (UnknownHostException e) {
            throw new TransientDeliveryException("Could not resolve webhook host " + uri.getHost(), e);
        }
        for (InetAddress address : addresses) {
            if (isPrivate(address)) {
                throw notAllowed(url, "resolves to a non-public address");
            }
        }
        return uri;
    }

    static boolean isPrivate(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return true;
        }
        byte[] b = address.getAddress();
        if (address instanceof Inet4Address) {
            int first = b[0] & 0xff;
            int second = b[1] & 0xff;
            return first == 0                                   // 0.0.0.0/8, "this network"
                    || (first == 100 && second >= 64 && second <= 127) // 100.64.0.0/10, carrier-grade NAT
                    || (first == 192 && second == 0 && (b[2] & 0xff) == 0) // 192.0.0.0/24, protocol assignments
                    || (first == 198 && (second == 18 || second == 19))    // 198.18.0.0/15, benchmarking
                    || first >= 240;                            // 240.0.0.0/4 reserved, and broadcast
        }
        if (address instanceof Inet6Address) {
            return (b[0] & 0xfe) == 0xfc;                       // fc00::/7, unique local addresses
        }
        return false;
    }

    private static PermanentDeliveryException notAllowed(String url, String reason) {
        return new PermanentDeliveryException(FailureCodes.TARGET_NOT_ALLOWED,
                "Webhook URL " + url + " " + reason, null);
    }
}
