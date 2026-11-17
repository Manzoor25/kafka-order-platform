package io.orderplatform.common.idempotency;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Objects;

/**
 * Atomic idempotent consumer deduplication store backed by PostgreSQL {@code processed_events}.
 * Relies on {@code INSERT ... ON CONFLICT (event_id) DO NOTHING} inside the caller's database transaction.
 */
public class ProcessedEventStore {

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    }

    /**
     * Marks {@code eventId} as processed inside the active transaction.
     *
     * @return {@code true} if this is the first time {@code eventId} is processed; {@code false} if duplicate.
     */
    public boolean tryClaim(String eventId, String eventType, String orderId, String consumerGroup) {
        int inserted = jdbcTemplate.update(
                """
                INSERT INTO processed_events (event_id, event_type, order_id, consumer_group)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                eventId,
                eventType,
                orderId,
                consumerGroup
        );
        return inserted == 1;
    }

    public boolean isProcessed(String eventId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM processed_events WHERE event_id = ?",
                Integer.class,
                eventId
        );
        return count != null && count > 0;
    }
}
