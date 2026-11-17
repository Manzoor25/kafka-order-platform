package io.orderplatform.payment.gateway;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Simulates an external payment processor (e.g., Stripe / Adyen) whose side effects commit
 * OUTSIDE Kafka's transaction coordinator ({@code Propagation.REQUIRES_NEW}).
 *
 * <p>Demonstrates why Kafka's {@code read-process-write} transaction alone cannot prevent a double
 * charge on crash-before-commit, and how a deterministic {@code idempotencyKey} ("charge:" + orderId)
 * closes that boundary cleanly.</p>
 */
@Component
public class SimulatedCardGateway {

    public record GatewayResult(
            boolean approved,
            String referenceId,
            String reason,
            boolean deduplicatedFromPriorCall
    ) {
    }

    public record GatewayOperationAudit(
            String idempotencyKey,
            String orderId,
            String operationType,
            String gatewayReferenceId,
            long amountCents,
            String outcome,
            int executionCount,
            int deduplicatedRetryCount
    ) {
    }

    private final JdbcTemplate jdbcTemplate;

    public SimulatedCardGateway(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public GatewayResult authorize(
            String idempotencyKey,
            String orderId,
            long amountCents,
            String currency,
            String paymentMethodToken) {
        List<GatewayResult> existing = jdbcTemplate.query(
                """
                SELECT outcome, gateway_reference_id, reason
                FROM gateway_operations
                WHERE idempotency_key = ?
                FOR UPDATE
                """,
                (rs, rowNum) -> new GatewayResult(
                        "APPROVED".equals(rs.getString("outcome")),
                        rs.getString("gateway_reference_id"),
                        rs.getString("reason"),
                        true
                ),
                idempotencyKey
        );
        if (!existing.isEmpty()) {
            jdbcTemplate.update(
                    """
                    UPDATE gateway_operations
                    SET deduplicated_retry_count = deduplicated_retry_count + 1
                    WHERE idempotency_key = ?
                    """,
                    idempotencyKey
            );
            return existing.getFirst();
        }

        boolean declined = paymentMethodToken != null
                && (paymentMethodToken.startsWith("tok_decline")
                || paymentMethodToken.equalsIgnoreCase("DECLINE"));

        String refId = (declined ? "dec_" : "ch_") + UUID.randomUUID().toString().substring(0, 12);
        String outcome = declined ? "DECLINED" : "APPROVED";
        String reason = declined ? "Card declined by issuer (" + paymentMethodToken + ")" : null;

        jdbcTemplate.update(
                """
                INSERT INTO gateway_operations (
                    idempotency_key, order_id, operation_type, gateway_reference_id,
                    amount_cents, currency, outcome, reason, execution_count, deduplicated_retry_count
                ) VALUES (?, ?, 'CHARGE', ?, ?, ?, ?, ?, 1, 0)
                """,
                idempotencyKey, orderId, refId, amountCents, currency, outcome, reason
        );
        return new GatewayResult(!declined, refId, reason, false);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public GatewayResult refund(
            String idempotencyKey,
            String orderId,
            long amountCents,
            String currency,
            String reason) {
        List<GatewayResult> existing = jdbcTemplate.query(
                """
                SELECT gateway_reference_id, reason
                FROM gateway_operations
                WHERE idempotency_key = ?
                FOR UPDATE
                """,
                (rs, rowNum) -> new GatewayResult(
                        true,
                        rs.getString("gateway_reference_id"),
                        rs.getString("reason"),
                        true
                ),
                idempotencyKey
        );
        if (!existing.isEmpty()) {
            jdbcTemplate.update(
                    "UPDATE gateway_operations SET deduplicated_retry_count = deduplicated_retry_count + 1 WHERE idempotency_key = ?",
                    idempotencyKey
            );
            return existing.getFirst();
        }

        String refundId = "re_" + UUID.randomUUID().toString().substring(0, 12);
        jdbcTemplate.update(
                """
                INSERT INTO gateway_operations (
                    idempotency_key, order_id, operation_type, gateway_reference_id,
                    amount_cents, currency, outcome, reason, execution_count, deduplicated_retry_count
                ) VALUES (?, ?, 'REFUND', ?, ?, ?, 'REFUNDED', ?, 1, 0)
                """,
                idempotencyKey, orderId, refundId, amountCents, currency, reason
        );
        return new GatewayResult(true, refundId, reason, false);
    }

    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<GatewayOperationAudit> findOperationsForOrder(String orderId) {
        return jdbcTemplate.query(
                """
                SELECT idempotency_key, order_id, operation_type, gateway_reference_id,
                       amount_cents, outcome, execution_count, deduplicated_retry_count
                FROM gateway_operations
                WHERE order_id = ?
                ORDER BY created_at ASC
                """,
                (rs, rowNum) -> new GatewayOperationAudit(
                        rs.getString("idempotency_key"),
                        rs.getString("order_id"),
                        rs.getString("operation_type"),
                        rs.getString("gateway_reference_id"),
                        rs.getLong("amount_cents"),
                        rs.getString("outcome"),
                        rs.getInt("execution_count"),
                        rs.getInt("deduplicated_retry_count")
                ),
                orderId
        );
    }
}
