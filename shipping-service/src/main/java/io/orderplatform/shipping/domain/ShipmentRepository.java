package io.orderplatform.shipping.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Repository
public class ShipmentRepository {

    public record ShipmentOutcome(
            boolean scheduled,
            String shipmentId,
            String carrier,
            String trackingNumber,
            String failureReason
    ) {
    }

    private final JdbcTemplate jdbcTemplate;

    public ShipmentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public ShipmentOutcome bookOrRejectIdempotently(
            String orderId,
            String reservationId,
            String sku,
            int quantity,
            String postalCode,
            boolean carrierRejected,
            String rejectionReason) {
        List<ShipmentOutcome> existing = jdbcTemplate.query(
                """
                SELECT shipment_id, carrier, tracking_number, status, failure_reason
                FROM shipments
                WHERE order_id = ?
                FOR UPDATE
                """,
                (rs, rowNum) -> new ShipmentOutcome(
                        "SCHEDULED".equals(rs.getString("status")),
                        rs.getString("shipment_id"),
                        rs.getString("carrier"),
                        rs.getString("tracking_number"),
                        rs.getString("failure_reason")
                ),
                orderId
        );
        if (!existing.isEmpty()) {
            return existing.getFirst();
        }

        String shipmentId = "ship-" + UUID.nameUUIDFromBytes(("ship:" + orderId).getBytes(StandardCharsets.UTF_8));
        if (carrierRejected) {
            jdbcTemplate.update(
                    """
                    INSERT INTO shipments (
                        shipment_id, order_id, reservation_id, sku, quantity,
                        carrier, tracking_number, shipping_postal_code, status, failure_reason
                    ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'FAILED', ?)
                    """,
                    shipmentId, orderId, reservationId, sku, quantity, postalCode, rejectionReason
            );
            return new ShipmentOutcome(false, shipmentId, null, null, rejectionReason);
        }

        String carrier = Math.abs(orderId.hashCode()) % 2 == 0 ? "UPS" : "DHL";
        String trackingNumber = carrier + "-" + postalCode + "-"
                + UUID.nameUUIDFromBytes(orderId.getBytes(StandardCharsets.UTF_8)).toString().substring(0, 8).toUpperCase();
        jdbcTemplate.update(
                """
                INSERT INTO shipments (
                    shipment_id, order_id, reservation_id, sku, quantity,
                    carrier, tracking_number, shipping_postal_code, status, failure_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SCHEDULED', NULL)
                """,
                shipmentId, orderId, reservationId, sku, quantity, carrier, trackingNumber, postalCode
        );
        return new ShipmentOutcome(true, shipmentId, carrier, trackingNumber, null);
    }

    @Transactional
    public void recordDeadLetterObservation(
            String dltTopic,
            int partition,
            long offset,
            String key,
            String payloadType,
            String exceptionMessage) {
        jdbcTemplate.update(
                """
                INSERT INTO dead_letter_audit (
                    dlt_topic, dlt_partition, dlt_offset, record_key, payload_type, exception_message
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                dltTopic, partition, offset, key, payloadType, exceptionMessage
        );
    }

    public int countDeadLettersForKey(String key) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dead_letter_audit WHERE record_key = ?",
                Integer.class,
                key
        );
        return count == null ? 0 : count;
    }
}
