package com.eventnotification.worker.delivery;

/** Sends one notification over one channel, such as email or SMS. */
public interface ChannelSender {

    /**
     * @throws PermanentDeliveryException if retrying cannot help, such as a rejected recipient
     * @throws TransientDeliveryException if a retry may succeed, such as a provider outage
     */
    void send(NotificationMessage message);
}
