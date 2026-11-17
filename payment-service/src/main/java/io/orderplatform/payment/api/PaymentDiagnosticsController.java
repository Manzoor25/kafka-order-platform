package io.orderplatform.payment.api;

import io.orderplatform.payment.domain.PaymentLedgerRepository;
import io.orderplatform.payment.domain.PaymentLedgerRepository.PaymentRecord;
import io.orderplatform.payment.gateway.SimulatedCardGateway;
import io.orderplatform.payment.gateway.SimulatedCardGateway.GatewayOperationAudit;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only diagnostic inspection endpoint used ONLY by failure-demo verification scripts
 * to prove gateway charge counts after killing payment-service mid-transaction.
 * Never invoked by peer microservices.
 */
@RestController
@RequestMapping("/internal/diagnostics/payments")
public class PaymentDiagnosticsController {

    public record PaymentAuditView(
            PaymentRecord payment,
            List<GatewayOperationAudit> gatewayOperations
    ) {
    }

    private final PaymentLedgerRepository ledgerRepository;
    private final SimulatedCardGateway cardGateway;

    public PaymentDiagnosticsController(
            PaymentLedgerRepository ledgerRepository,
            SimulatedCardGateway cardGateway) {
        this.ledgerRepository = ledgerRepository;
        this.cardGateway = cardGateway;
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<PaymentAuditView> inspectOrderPayment(@PathVariable String orderId) {
        List<GatewayOperationAudit> gatewayOps = cardGateway.findOperationsForOrder(orderId);
        PaymentRecord payment = ledgerRepository.findByOrderId(orderId).orElse(null);
        if (payment == null && gatewayOps.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new PaymentAuditView(payment, gatewayOps));
    }
}
