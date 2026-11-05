package com.eventnotification.api.error;

import com.eventnotification.api.idempotency.IdempotencyKeyReusedException;
import com.eventnotification.api.idempotency.InvalidIdempotencyKeyException;
import com.eventnotification.api.notification.NotificationNotFoundException;
import com.eventnotification.api.notification.UserIds;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.Map;

/** Returns RFC 9457 problem details for every error, with per-field messages for validation errors. */
@RestControllerAdvice
class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(NotificationNotFoundException.class)
    ProblemDetail handleNotFound(NotificationNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(UserIds.InvalidUserIdException.class)
    ProblemDetail handleInvalidUserId(UserIds.InvalidUserIdException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(InvalidIdempotencyKeyException.class)
    ProblemDetail handleInvalidIdempotencyKey(InvalidIdempotencyKeyException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /** 422, as in the IETF Idempotency-Key draft: the request is well formed but conflicts with the key. */
    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ProblemDetail handleIdempotencyKeyReused(IdempotencyKeyReusedException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage());
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request validation failed");
        List<Map<String, String>> errors = ex.getBindingResult().getAllErrors().stream()
                .map(ApiExceptionHandler::toFieldError)
                .toList();
        problem.setProperty("errors", errors);
        return ResponseEntity.badRequest().body(problem);
    }

    private static Map<String, String> toFieldError(ObjectError error) {
        String field = error instanceof FieldError fe ? fe.getField() : error.getObjectName();
        String message = error.getDefaultMessage() == null ? "is invalid" : error.getDefaultMessage();
        return Map.of("field", field, "message", message);
    }
}
