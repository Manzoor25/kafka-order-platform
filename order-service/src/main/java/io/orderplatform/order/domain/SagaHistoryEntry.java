package io.orderplatform.order.domain;

import java.time.Instant;

public record SagaHistoryEntry(
        long id,
        String orderId,
        String fromStatus,
        String toStatus,
        String triggerEvent,
        String detail,
        Instant occurredAt
) {
}
