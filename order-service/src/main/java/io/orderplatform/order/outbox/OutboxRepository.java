package io.orderplatform.order.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {

    private static final RowMapper<OutboxEventRecord> ROW_MAPPER = (rs, rowNum) -> new OutboxEventRecord(
            rs.getObject("id", UUID.class),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getString("topic"),
            rs.getString("event_type"),
            rs.getString("payload_json"),
            rs.getString("traceparent"),
            toInstant(rs.getTimestamp("created_at")),
            toInstant(rs.getTimestamp("published_at")),
            rs.getInt("attempt_count"),
            rs.getString("last_error")
    );

    private final JdbcTemplate jdbcTemplate;

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(
            UUID id,
            String aggregateType,
            String aggregateId,
            String topic,
            String eventType,
            String payloadJson,
            String traceparent) {
        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (id, aggregate_type, aggregate_id, topic, event_type, payload_json, traceparent)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                id,
                aggregateType,
                aggregateId,
                topic,
                eventType,
                payloadJson,
                traceparent
        );
    }

    public List<OutboxEventRecord> claimUnpublishedBatch(int batchSize) {
        return jdbcTemplate.query(
                """
                SELECT id, aggregate_type, aggregate_id, topic, event_type, payload_json, traceparent,
                       created_at, published_at, attempt_count, last_error
                FROM outbox_events
                WHERE published_at IS NULL
                ORDER BY created_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """,
                ROW_MAPPER,
                batchSize
        );
    }

    public void markPublished(UUID id, Instant publishedAt) {
        jdbcTemplate.update(
                """
                UPDATE outbox_events
                SET published_at = ?, attempt_count = attempt_count + 1, last_error = NULL
                WHERE id = ?
                """,
                Timestamp.from(publishedAt),
                id
        );
    }

    public void recordFailure(UUID id, String errorMessage) {
        String truncated = errorMessage == null ? "unknown"
                : errorMessage.substring(0, Math.min(errorMessage.length(), 500));
        jdbcTemplate.update(
                """
                UPDATE outbox_events
                SET attempt_count = attempt_count + 1, last_error = ?
                WHERE id = ?
                """,
                truncated,
                id
        );
    }

    public List<OutboxEventRecord> findByAggregateId(String aggregateId) {
        return jdbcTemplate.query(
                """
                SELECT id, aggregate_type, aggregate_id, topic, event_type, payload_json, traceparent,
                       created_at, published_at, attempt_count, last_error
                FROM outbox_events
                WHERE aggregate_id = ?
                ORDER BY created_at ASC
                """,
                ROW_MAPPER,
                aggregateId
        );
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
