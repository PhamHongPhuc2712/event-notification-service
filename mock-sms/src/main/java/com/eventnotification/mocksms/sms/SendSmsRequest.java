package com.eventnotification.mocksms.sms;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record SendSmsRequest(
        @NotBlank @Pattern(regexp = "\\+[1-9][0-9]{6,14}", message = "must be an E.164 phone number")
        String to,
        @NotBlank @Size(max = 1_600)
        String body) {
}
