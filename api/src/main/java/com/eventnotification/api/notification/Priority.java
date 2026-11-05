package com.eventnotification.api.notification;

/**
 * How urgent a notification is. HIGH is for messages the user is waiting for, such as a
 * password reset or a login code: they travel on their own topics, skip quiet hours and have
 * their own rate limit, so a marketing backlog cannot hold them up.
 */
public enum Priority {
    NORMAL,
    HIGH;

    /** HIGH notifications use the channel's topic with this suffix, such as notifications.sms.high. */
    public static final String HIGH_TOPIC_SUFFIX = ".high";

    public String topic(String channelTopic) {
        return this == HIGH ? channelTopic + HIGH_TOPIC_SUFFIX : channelTopic;
    }
}
