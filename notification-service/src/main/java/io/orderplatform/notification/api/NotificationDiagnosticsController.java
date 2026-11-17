package io.orderplatform.notification.api;

import io.orderplatform.notification.domain.NotificationRepository;
import io.orderplatform.notification.domain.NotificationRepository.NotificationEntry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only diagnostic endpoint used ONLY by E2E tests and demo verification scripts.
 * Never called by peer microservices.
 */
@RestController
@RequestMapping("/internal/diagnostics/notifications")
public class NotificationDiagnosticsController {

    private final NotificationRepository repository;

    public NotificationDiagnosticsController(NotificationRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/{orderId}")
    public List<NotificationEntry> getForOrder(@PathVariable String orderId) {
        return repository.findByOrderId(orderId);
    }
}
