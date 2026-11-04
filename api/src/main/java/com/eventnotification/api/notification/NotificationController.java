package com.eventnotification.api.notification;

import com.eventnotification.api.idempotency.IdempotencyKey;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/notifications")
class NotificationController {

    private final NotificationService service;

    NotificationController(NotificationService service) {
        this.service = service;
    }

    /** Set on responses replayed from an earlier request with the same idempotency key. */
    static final String REPLAYED_HEADER = "Idempotent-Replayed";

    /**
     * Returns 202 Accepted: the notification is stored and will be delivered asynchronously.
     * A replayed request also returns 202, with the notification's current state.
     */
    @PostMapping
    ResponseEntity<NotificationResponse> create(@Valid @RequestBody CreateNotificationRequest request,
                                                @RequestHeader(name = IdempotencyKey.HEADER, required = false)
                                                String idempotencyKey) {
        IdempotencyKey key = idempotencyKey == null ? null : new IdempotencyKey(idempotencyKey);
        CreateResult result = service.create(request, key);
        Notification notification = result.notification();
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(notification.id())
                .toUri();
        ResponseEntity.BodyBuilder response = ResponseEntity.accepted().location(location);
        if (result.replayed()) {
            response.header(REPLAYED_HEADER, "true");
        }
        return response.body(NotificationResponse.from(notification));
    }

    @GetMapping("/{id}")
    NotificationResponse get(@PathVariable UUID id) {
        return NotificationResponse.from(service.get(id));
    }
}
