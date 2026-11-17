package io.orderplatform.order.api;

import io.orderplatform.order.domain.CreateOrderCommand;
import io.orderplatform.order.domain.OrderRecord;
import io.orderplatform.order.domain.OrderSagaService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * External client ingress API for submitting orders and polling read-model status.
 * Peer microservices NEVER call this controller; all inter-service communication is over Kafka.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderSagaService orderSagaService;

    public OrderController(OrderSagaService orderSagaService) {
        this.orderSagaService = orderSagaService;
    }

    @PostMapping
    public ResponseEntity<OrderDetailResponse> createOrder(@Valid @RequestBody CreateOrderRequest req) {
        OrderRecord created = orderSagaService.createOrder(new CreateOrderCommand(
                req.orderId(),
                req.customerId(),
                req.customerEmail(),
                req.sku(),
                req.quantity(),
                req.totalAmountCents(),
                req.currency(),
                req.paymentMethodToken(),
                req.shippingPostalCode(),
                req.couponCode()
        ));
        OrderDetailResponse body = OrderDetailResponse.from(
                created, orderSagaService.findHistory(created.orderId()));
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.CREATED);
        if (created.traceId() != null) {
            builder.header("X-Trace-Id", created.traceId());
        }
        return builder.body(body);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderDetailResponse> getOrder(@PathVariable String orderId) {
        return orderSagaService.findOrder(orderId)
                .map(order -> ResponseEntity.ok(
                        OrderDetailResponse.from(order, orderSagaService.findHistory(orderId))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<OrderDetailResponse> listOrders(@RequestParam(defaultValue = "25") int limit) {
        int capped = Math.max(1, Math.min(limit, 100));
        return orderSagaService.findRecentOrders(capped).stream()
                .map(o -> OrderDetailResponse.from(o, List.of()))
                .toList();
    }
}
