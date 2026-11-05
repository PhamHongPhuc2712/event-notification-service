package com.eventnotification.api.preference;

import com.eventnotification.api.notification.UserIds;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;

/** Preferences are checked by the worker at delivery time, so a change applies to queued notifications too. */
@RestController
@RequestMapping("/users/{userId}/preferences")
class UserPreferencesController {

    private final UserPreferencesRepository repository;
    private final Clock clock;

    UserPreferencesController(UserPreferencesRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /** Returns the defaults (every channel, no quiet hours) for a user who never set any. */
    @GetMapping
    UserPreferences get(@PathVariable String userId) {
        UserIds.requireValid(userId);
        return repository.find(userId).orElseGet(UserPreferences::defaults);
    }

    @PutMapping
    UserPreferences put(@PathVariable String userId, @Valid @RequestBody UserPreferences preferences) {
        UserIds.requireValid(userId);
        repository.save(userId, preferences, clock.instant());
        return preferences;
    }
}
