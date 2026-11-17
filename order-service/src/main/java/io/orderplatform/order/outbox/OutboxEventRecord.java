package io.orderplatform.order.outbox;

import java.time.Instant;
import java.util.UUID;

public record OutboxEventRecord(
        UUID id,
        String aggregateType,
        String aggregateId,
        String topic,
        String eventType,
        String payloadJson,
        String traceparent,
        Instant createdAt,
        Instant publishedAt,
        int attemptCount,
        String lastError
) {
}
