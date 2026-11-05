package com.eventnotification.api.notification;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks the rules that depend on the channel: the recipient's format (email address, phone
 * number or webhook URL) and the SMS length limit.
 * Errors are reported on the {@code recipient} and {@code body} fields, not on the class.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = ValidForChannel.Validator.class)
@interface ValidForChannel {

    String message() default "is invalid for the channel";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<ValidForChannel, CreateNotificationRequest> {

        private static final Pattern EMAIL = Pattern.compile("[^@\\s]+@[^@\\s]+\\.[^@\\s]+");
        private static final Pattern E164 = Pattern.compile("\\+[1-9][0-9]{6,14}");
        static final int MAX_SMS_LENGTH = 1_600;
        /** Plain http is only for a receiver on the developer's own machine. */
        private static final Set<String> LOCAL_HOSTS = Set.of("localhost", "127.0.0.1");

        @Override
        public boolean isValid(CreateNotificationRequest request, ConstraintValidatorContext context) {
            if (request == null || request.channel() == null) {
                return true;
            }
            context.disableDefaultConstraintViolation();
            boolean valid = true;
            if (request.recipient() != null && !recipientMatches(request)) {
                valid = violation(context, "recipient", switch (request.channel()) {
                    case EMAIL -> "must be a well-formed email address";
                    case SMS -> "must be an E.164 phone number, such as +6591234567";
                    case WEBHOOK -> "must be an https URL without credentials or a fragment";
                });
            }
            if (request.channel() == Channel.SMS && request.body() != null
                    && request.body().length() > MAX_SMS_LENGTH) {
                valid = violation(context, "body", "must be at most " + MAX_SMS_LENGTH + " characters for SMS");
            }
            return valid;
        }

        private static boolean recipientMatches(CreateNotificationRequest request) {
            return switch (request.channel()) {
                case EMAIL -> EMAIL.matcher(request.recipient()).matches();
                case SMS -> E164.matcher(request.recipient()).matches();
                case WEBHOOK -> isWebhookUrl(request.recipient());
            };
        }

        /**
         * Checks the URL's shape only. Whether the host is allowed, such as not a private
         * address, is checked by the worker when it sends, after resolving the name.
         */
        static boolean isWebhookUrl(String value) {
            try {
                URI uri = new URI(value);
                String host = uri.getHost();
                if (host == null || uri.getRawUserInfo() != null || uri.getRawFragment() != null) {
                    return false;
                }
                return "https".equals(uri.getScheme())
                        || ("http".equals(uri.getScheme()) && LOCAL_HOSTS.contains(host));
            } catch (URISyntaxException e) {
                return false;
            }
        }

        private static boolean violation(ConstraintValidatorContext context, String field, String message) {
            context.buildConstraintViolationWithTemplate(message).addPropertyNode(field).addConstraintViolation();
            return false;
        }
    }
}
