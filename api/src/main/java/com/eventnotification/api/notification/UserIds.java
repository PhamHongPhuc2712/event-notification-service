package com.eventnotification.api.notification;

import java.util.regex.Pattern;

/** The user ID format, shared by request bodies and URL paths. */
public final class UserIds {

    public static final String PATTERN = "[A-Za-z0-9_-]{1,64}";
    private static final Pattern VALID = Pattern.compile(PATTERN);

    private UserIds() {
    }

    /** @throws InvalidUserIdException if the ID is not 1 to 64 letters, digits, underscores or hyphens */
    public static void requireValid(String userId) {
        if (userId == null || !VALID.matcher(userId).matches()) {
            throw new InvalidUserIdException();
        }
    }

    public static class InvalidUserIdException extends RuntimeException {

        InvalidUserIdException() {
            super("userId must be 1 to 64 letters, digits, underscores or hyphens");
        }
    }
}
