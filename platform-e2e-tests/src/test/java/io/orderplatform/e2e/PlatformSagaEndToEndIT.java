package io.orderplatform.e2e;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.orderplatform.dlq.DeadLetterInspectorAndReplayer;
import io.orderplatform.dlq.DlqCliOptions;
import io.orderplatform.events.TopicNames;
import io.orderplatform.inventory.InventoryServiceApplication;
import io.orderplatform.inventory.domain.InventoryStockRepository;
import io.orderplatform.notification.NotificationServiceApplication;
import io.orderplatform.notification.domain.NotificationRepository;
import io.orderplatform.notification.domain.NotificationRepository.NotificationEntry;
import io.orderplatform.order.OrderServiceApplication;
import io.orderplatform.order.api.CreateOrderRequest;
import io.orderplatform.order.api.OrderDetailResponse;
import io.orderplatform.payment.PaymentServiceApplication;
import io.orderplatform.payment.domain.PaymentProcessorService;
import io.orderplatform.payment.gateway.SimulatedCardGateway;
import io.orderplatform.payment.gateway.SimulatedCardGateway.GatewayOperationAudit;
import io.orderplatform.shipping.ShippingServiceApplication;
import io.orderplatform.shipping.domain.ShippingBookingService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full 5-microservice end-to-end Testcontainers verification covering:
 *  1. Happy path saga across order -> payment -> inventory -> shipping -> order -> notification
 *     plus 5-service OpenTelemetry traceId continuity.
 *  2. Compensation Path A: Payment declined -> OrderCancelled.
 *  3. Compensation Path B: Out-of-stock -> PaymentRefunded -> OrderCancelled.
 *  4. Compensation Path C: Shipping rejected -> InventoryReleased -> PaymentRefunded -> OrderCancelled.
 *  5. Mid-transaction crash in payment-service (single charge guarantee) + Poison message to DLT + CLI replay.
 */
@Testcontainers
class PlatformSagaEndToEndIT {

    public record RecordedServiceSpan(String serviceName, String traceId, String spanName) {
    }

    static final List<RecordedServiceSpan> RECORDED_SPANS = new CopyOnWriteArrayList<>();

    @Configuration
    static class SharedInMemorySpanCollectorConfig {
        @Bean
        public SpanExporter sharedE2eSpanExporter(@Value("${spring.application.name}") String serviceName) {
            return new SpanExporter() {
                @Override
                public CompletableResultCode export(Collection<SpanData> spans) {
                    for (SpanData span : spans) {
                        RECORDED_SPANS.add(new RecordedServiceSpan(serviceName, span.getTraceId(), span.getName()));
                    }
                    return CompletableResultCode.ofSuccess();
                }

                @Override
                public CompletableResultCode flush() {
                    return CompletableResultCode.ofSuccess();
                }

                @Override
                public CompletableResultCode shutdown() {
                    return CompletableResultCode.ofSuccess();
                }
            };
        }
    }

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("postgres")
            .withUsername("platform_admin")
            .withPassword("platform_pass");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    @Container
    static final GenericContainer<?> APICURIO = new GenericContainer<>("apicurio/apicurio-registry:3.3.4")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200));

    private static final List<ConfigurableApplicationContext> CONTEXTS = new ArrayList<>();
    private static ConfigurableApplicationContext orderCtx;
    private static ConfigurableApplicationContext paymentCtx;
    private static ConfigurableApplicationContext inventoryCtx;
    private static ConfigurableApplicationContext notificationCtx;
    private static RestClient orderApi;
    private static String registryUrl;

    @BeforeAll
    static void startAllFiveMicroservices() throws Exception {
        createIsolatedDatabases();
        registryUrl = "http://" + APICURIO.getHost() + ":" + APICURIO.getMappedPort(8080) + "/apis/registry/v3";

        orderCtx = bootService(OrderServiceApplication.class, "order_db", "order.outbox.poll-interval-ms=100");
        paymentCtx = bootService(PaymentServiceApplication.class, "payment_db", "payment.fault-injection.mid-tx-hold-ms=0");
        inventoryCtx = bootService(InventoryServiceApplication.class, "inventory_db");
        bootService(ShippingServiceApplication.class, "shipping_db",
                "shipping.retry.initial-delay-ms=150", "shipping.retry.max-delay-ms=500");
        notificationCtx = bootService(NotificationServiceApplication.class, "notification_db");

        int orderPort = Integer.parseInt(orderCtx.getEnvironment().getProperty("local.server.port", "8080"));
        orderApi = RestClient.create("http://localhost:" + orderPort);
    }

    @AfterAll
    static void stopAllFiveMicroservices() {
        for (int i = CONTEXTS.size() - 1; i >= 0; i--) {
            try {
                CONTEXTS.get(i).close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void happyPathCompletesOrderAcrossAllFiveServicesWithSingleDistributedTraceId() {
        String orderId = "ord-e2e-happy-" + UUID.randomUUID();
        OrderDetailResponse created = postOrder(new CreateOrderRequest(
                orderId, "cust-happy", "happy@example.com", "SKU-LAPTOP-PRO",
                1, 249900L, "USD", "tok_visa_valid", "98101", "PORTFOLIO10"));

        assertThat(created.traceId()).hasSize(32);

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            OrderDetailResponse completed = getOrder(orderId);
            assertThat(completed.status()).isEqualTo("COMPLETED");
            assertThat(completed.paymentId()).isNotBlank();
            assertThat(completed.reservationId()).isNotBlank();
            assertThat(completed.shipmentId()).isNotBlank();
            assertThat(completed.trackingNumber()).isNotBlank();
        });

        NotificationRepository notifications = notificationCtx.getBean(NotificationRepository.class);
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<NotificationEntry> sent = notifications.findByOrderId(orderId);
            assertThat(sent).extracting(NotificationEntry::notificationType)
                    .contains("SHIPMENT_DISPATCHED", "ORDER_CONFIRMED");
            assertThat(sent).filteredOn(n -> "ORDER_CONFIRMED".equals(n.notificationType()))
                    .singleElement()
                    .satisfies(n -> assertThat(n.traceId()).isEqualTo(created.traceId()));
        });

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Set<String> servicesWithTrace = RECORDED_SPANS.stream()
                    .filter(s -> created.traceId().equals(s.traceId()))
                    .map(RecordedServiceSpan::serviceName)
                    .collect(Collectors.toSet());
            assertThat(servicesWithTrace).contains(
                    "order-service",
                    "payment-service",
                    "inventory-service",
                    "shipping-service",
                    "notification-service"
            );
        });
    }

    @Test
    void allThreeSagaFailurePathsCompensateCleanlyAndLeaveZeroLeakedStockOrCharges() {
        SimulatedCardGateway gateway = paymentCtx.getBean(SimulatedCardGateway.class);
        InventoryStockRepository stockRepo = inventoryCtx.getBean(InventoryStockRepository.class);
        NotificationRepository notifications = notificationCtx.getBean(NotificationRepository.class);

        // Path A: Payment Declined
        String declinedOrder = "ord-e2e-dec-" + UUID.randomUUID();
        postOrder(new CreateOrderRequest(
                declinedOrder, "cust-dec", "dec@example.com", "SKU-MOUSE",
                1, 4900L, "USD", "tok_decline_card", "98101", null));

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            OrderDetailResponse order = getOrder(declinedOrder);
            assertThat(order.status()).isEqualTo("CANCELLED");
            assertThat(order.failureStage()).isEqualTo("PAYMENT");
        });

        // Path B: Out-of-Stock -> PaymentRefunded -> OrderCancelled
        String oosOrder = "ord-e2e-oos-" + UUID.randomUUID();
        postOrder(new CreateOrderRequest(
                oosOrder, "cust-oos", "oos@example.com", "SKU-OUT-OF-STOCK",
                1, 3900L, "USD", "tok_visa_ok", "98101", null));

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            OrderDetailResponse order = getOrder(oosOrder);
            assertThat(order.status()).isEqualTo("CANCELLED");
            assertThat(order.failureStage()).isEqualTo("INVENTORY_REJECTED");
            assertThat(gateway.findOperationsForOrder(oosOrder))
                    .extracting(GatewayOperationAudit::operationType)
                    .containsExactly("CHARGE", "REFUND");
        });

        // Path C: Shipping Rejected -> InventoryReleased (restores SKU-LIMITED-1 to 1) -> PaymentRefunded -> OrderCancelled
        int stockBefore = stockRepo.findStockBySku("SKU-LIMITED-1").orElseThrow().availableQty();
        assertThat(stockBefore).isEqualTo(1);

        String shipFailOrder = "ord-e2e-shipfail-" + UUID.randomUUID();
        postOrder(new CreateOrderRequest(
                shipFailOrder, "cust-sf", "sf@example.com", "SKU-LIMITED-1",
                1, 9900L, "USD", "tok_visa_ok", "FAIL-SHIP-ZONE", null));

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            OrderDetailResponse order = getOrder(shipFailOrder);
            assertThat(order.status()).isEqualTo("CANCELLED");
            assertThat(order.failureStage()).isEqualTo("SHIPPING_FAILED");
            assertThat(stockRepo.findStockBySku("SKU-LIMITED-1").orElseThrow().availableQty()).isEqualTo(1);
            assertThat(gateway.findOperationsForOrder(shipFailOrder))
                    .extracting(GatewayOperationAudit::operationType)
                    .containsExactly("CHARGE", "REFUND");
            assertThat(notifications.findByOrderId(shipFailOrder))
                    .extracting(NotificationEntry::notificationType)
                    .contains("PAYMENT_REFUNDED", "ORDER_CANCELLED");
        });
    }

    @Test
    void midTxCrashChargesCustomerOnceAndPoisonOrderRecoversViaDlqCliReplay() {
        SimulatedCardGateway gateway = paymentCtx.getBean(SimulatedCardGateway.class);

        // 1. Mid-Kafka-TX crash in payment-service -> single charge + saga completes
        String crashOrder = "ord-e2e-midtx-" + UUID.randomUUID();
        postOrder(new CreateOrderRequest(
                crashOrder, "cust-eos", "eos@example.com", "SKU-HEADSET",
                1, 15900L, "USD", PaymentProcessorService.TOKEN_ABORT_MID_TX_ONCE, "98101", null));

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            OrderDetailResponse order = getOrder(crashOrder);
            assertThat(order.status()).isEqualTo("COMPLETED");
            List<GatewayOperationAudit> ops = gateway.findOperationsForOrder(crashOrder);
            assertThat(ops).hasSize(1);
            assertThat(ops.getFirst().executionCount()).isEqualTo(1);
            assertThat(ops.getFirst().deduplicatedRetryCount()).isGreaterThanOrEqualTo(1);
        });

        // 2. Poison order lands in inventory.reserved.v1.dlt, then recovers when replayed via dlq-cli
        String poisonOrder = "ord-e2e-poison-" + UUID.randomUUID();
        postOrder(new CreateOrderRequest(
                poisonOrder, "cust-dlq", "dlq@example.com", "SKU-MECH-KB",
                1, 12900L, "USD", "tok_visa_ok", ShippingBookingService.POSTAL_POISON_TRANSIENT, null));

        String dltTopic = TopicNames.dltTopic(TopicNames.INVENTORY_RESERVED);
        DeadLetterInspectorAndReplayer dlqEngine = new DeadLetterInspectorAndReplayer();

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            var deadLetters = dlqEngine.inspect(DlqCliOptions.parse(new String[]{
                    "inspect", "--topic", dltTopic, "--key", poisonOrder,
                    "--bootstrap-servers", KAFKA.getBootstrapServers(),
                    "--schema-registry-url", registryUrl
            }));
            assertThat(deadLetters).hasSize(1);
        });

        var replayResult = dlqEngine.replay(DlqCliOptions.parse(new String[]{
                "replay", "--topic", dltTopic, "--key", poisonOrder,
                "--bootstrap-servers", KAFKA.getBootstrapServers(),
                "--schema-registry-url", registryUrl
        }));
        assertThat(replayResult.replayedCount()).isEqualTo(1);

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(getOrder(poisonOrder).status()).isEqualTo("COMPLETED"));
    }

    private static OrderDetailResponse postOrder(CreateOrderRequest req) {
        OrderDetailResponse resp = orderApi.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .body(req)
                .retrieve()
                .body(OrderDetailResponse.class);
        assertThat(resp).isNotNull();
        return resp;
    }

    private static OrderDetailResponse getOrder(String orderId) {
        OrderDetailResponse resp = orderApi.get()
                .uri("/api/orders/{id}", orderId)
                .retrieve()
                .body(OrderDetailResponse.class);
        assertThat(resp).isNotNull();
        return resp;
    }

    private static void createIsolatedDatabases() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement stmt = conn.createStatement()) {
            for (String db : List.of("order_db", "payment_db", "inventory_db", "shipping_db", "notification_db")) {
                stmt.execute("CREATE DATABASE " + db);
            }
        }
    }

    private static ConfigurableApplicationContext bootService(
            Class<?> appClass,
            String databaseName,
            String... extraProperties) {
        String baseJdbc = POSTGRES.getJdbcUrl();
        String serviceJdbc = baseJdbc.substring(0, baseJdbc.lastIndexOf('/') + 1) + databaseName;

        List<String> args = new ArrayList<>(List.of(
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.main.banner-mode=off",
                "--spring.datasource.url=" + serviceJdbc,
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--platform.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--platform.kafka.schema-registry-url=" + registryUrl,
                "--platform.kafka.topic-replicas=1",
                "--platform.kafka.topic-min-insync-replicas=1",
                "--management.tracing.export.otlp.enabled=false",
                "--management.opentelemetry.tracing.export.schedule-delay=200ms"
        ));
        for (String extra : extraProperties) {
            args.add("--" + extra);
        }

        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(
                appClass, SharedInMemorySpanCollectorConfig.class)
                .run(args.toArray(String[]::new));
        CONTEXTS.add(ctx);
        return ctx;
    }
}
