package com.eventnotification.api.notification;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.regex.Pattern;

/**
 * Checks the rules that depend on the channel: the recipient's format and the SMS length limit.
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
            };
        }

        private static boolean violation(ConstraintValidatorContext context, String field, String message) {
            context.buildConstraintViolationWithTemplate(message).addPropertyNode(field).addConstraintViolation();
            return false;
        }
    }
}
