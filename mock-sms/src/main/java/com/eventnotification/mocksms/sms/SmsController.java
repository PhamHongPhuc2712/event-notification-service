package com.eventnotification.mocksms.sms;

import com.eventnotification.mocksms.faults.FaultInjector;
import com.eventnotification.mocksms.faults.FaultInjector.Fault;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * A fake SMS provider API. It requires an {@code Idempotency-Key} and accepts each key once, so
 * a client that retries after a timeout does not text the user twice.
 */
@RestController
@RequestMapping("/v1/messages")
class SmsController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String REPLAYED = "Idempotent-Replayed";
    /** Numbers with this prefix are rejected, to exercise permanent failures. */
    static final String UNREACHABLE_PREFIX = "+999";

    record SendSmsResponse(UUID id, String status) {
    }

    private final SmsStore store;
    private final FaultInjector faults;

    SmsController(SmsStore store, FaultInjector faults) {
        this.store = store;
        this.faults = faults;
    }

    @PostMapping
    ResponseEntity<?> send(@RequestHeader(name = IDEMPOTENCY_KEY, required = false) String idempotencyKey,
                           @Valid @RequestBody SendSmsRequest request) throws InterruptedException {
        store.recordRequest();
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 255) {
            return problem(HttpStatus.BAD_REQUEST, IDEMPOTENCY_KEY + " header of 1 to 255 characters is required");
        }
        if (request.to().startsWith(UNREACHABLE_PREFIX)) {
            return problem(HttpStatus.BAD_REQUEST, "Number is unreachable");
        }

        Fault fault = faults.next();
        if (fault != Fault.NONE) {
            store.recordFault(fault);
        }
        if (fault == Fault.FAILURE) {
            return problem(HttpStatus.SERVICE_UNAVAILABLE, "Injected failure");
        }

        SmsStore.Accepted accepted = store.accept(idempotencyKey, request);
        switch (fault) {
            case TIMEOUT -> sleep(faults.settings().timeoutDelay());
            case SLOW -> sleep(faults.settings().slowDelay());
            default -> {
            }
        }
        ResponseEntity.BodyBuilder response = ResponseEntity.accepted();
        if (accepted.replayed()) {
            response.header(REPLAYED, "true");
        }
        return response.body(new SendSmsResponse(accepted.message().id(), "accepted"));
    }

    @GetMapping
    List<SmsMessage> messages() {
        return store.messages();
    }

    @GetMapping("/stats")
    SmsStats stats() {
        return store.stats();
    }

    /** Clears messages and counters between test runs. */
    @DeleteMapping
    ResponseEntity<Void> reset() {
        store.reset();
        return ResponseEntity.noContent().build();
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ResponseEntity<ProblemDetail> handleKeyReused(IdempotencyKeyReusedException e) {
        return problem(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage());
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String detail) {
        return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, detail));
    }

    private static void sleep(Duration delay) throws InterruptedException {
        Thread.sleep(delay);
    }
}
