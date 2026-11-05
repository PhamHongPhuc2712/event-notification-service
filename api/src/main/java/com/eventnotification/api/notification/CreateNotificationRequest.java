package com.eventnotification.api.notification;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

@ValidForChannel
public record CreateNotificationRequest(
        @NotBlank
        @Size(max = 64)
        @Pattern(regexp = "[A-Za-z0-9_-]+", message = "may only contain letters, digits, underscores and hyphens")
        String userId,
        @NotNull
        Channel channel,
        /** An email address for EMAIL, an E.164 phone number such as +6591234567 for SMS. */
        @NotBlank @Size(max = 320)
        String recipient,
        @Size(max = 200)
        @Pattern(regexp = "[^\\r\\n\\x00]*", message = "must not contain line breaks or null characters")
        String subject,
        @NotBlank @Size(max = 10_000)
        @Pattern(regexp = "[^\\x00]*", message = "must not contain null characters")
        String body,
        /** NORMAL when omitted. */
        Priority priority) {

    /** The pre-priority shape, kept for callers that do not set one. */
    public CreateNotificationRequest(String userId, Channel channel, String recipient, String subject, String body) {
        this(userId, channel, recipient, subject, body, null);
    }

    public Priority effectivePriority() {
        return priority == null ? Priority.NORMAL : priority;
    }

    @AssertTrue(message = "subject is required for EMAIL notifications")
    boolean isSubjectPresentForEmail() {
        return channel != Channel.EMAIL || (subject != null && !subject.isBlank());
    }
}
