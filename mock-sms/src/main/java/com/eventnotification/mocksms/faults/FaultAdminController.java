package com.eventnotification.mocksms.faults;

import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Lets tests and chaos runs change the fault rates without a restart. A test double: no auth. */
@RestController
@RequestMapping("/admin/faults")
class FaultAdminController {

    private final FaultInjector faults;

    FaultAdminController(FaultInjector faults) {
        this.faults = faults;
    }

    @GetMapping
    FaultSettings get() {
        return faults.settings();
    }

    @PutMapping
    FaultSettings update(@Valid @RequestBody FaultSettings settings) {
        faults.update(settings);
        return settings;
    }
}
