package io.orderplatform.notification.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public class NotificationRepository {

    public record NotificationEntry(
            String notificationId,
            String orderId,
            String recipientEmail,
            String notificationType,
            String subject,
            String body,
            String traceId,
            Instant sentAt
    ) {
    }

    private final JdbcTemplate jdbcTemplate;

    public NotificationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertIdempotently(
            String notificationId,
            String dedupKey,
            String orderId,
            String recipientEmail,
            String notificationType,
            String subject,
            String body,
            String traceId) {
        jdbcTemplate.update(
                """
                INSERT INTO customer_notifications (
                    notification_id, dedup_key, order_id, recipient_email,
                    notification_type, subject, body, trace_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (dedup_key) DO NOTHING
                """,
                notificationId, dedupKey, orderId, recipientEmail, notificationType, subject, body, traceId
        );
    }

    public List<NotificationEntry> findByOrderId(String orderId) {
        return jdbcTemplate.query(
                """
                SELECT notification_id, order_id, recipient_email, notification_type,
                       subject, body, trace_id, sent_at
                FROM customer_notifications
                WHERE order_id = ?
                ORDER BY sent_at ASC
                """,
                (rs, rowNum) -> new NotificationEntry(
                        rs.getString("notification_id"),
                        rs.getString("order_id"),
                        rs.getString("recipient_email"),
                        rs.getString("notification_type"),
                        rs.getString("subject"),
                        rs.getString("body"),
                        rs.getString("trace_id"),
                        rs.getTimestamp("sent_at").toInstant()
                ),
                orderId
        );
    }
}
