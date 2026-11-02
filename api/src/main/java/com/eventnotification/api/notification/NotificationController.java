package com.eventnotification.api.notification;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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

    /** Returns 202 Accepted: the notification is stored and will be delivered asynchronously. */
    @PostMapping
    ResponseEntity<NotificationResponse> create(@Valid @RequestBody CreateNotificationRequest request) {
        Notification notification = service.create(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(notification.id())
                .toUri();
        return ResponseEntity.accepted().location(location).body(NotificationResponse.from(notification));
    }

    @GetMapping("/{id}")
    NotificationResponse get(@PathVariable UUID id) {
        return NotificationResponse.from(service.get(id));
    }
}
