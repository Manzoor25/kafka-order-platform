package io.orderplatform.payment;

import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.PaymentDeclined;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.payment.domain.PaymentProcessorService;
import io.orderplatform.payment.gateway.SimulatedCardGateway;
import io.orderplatform.payment.gateway.SimulatedCardGateway.GatewayOperationAudit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PaymentServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("payment_db")
            .withUsername("payment_user")
            .withPassword("payment_pass");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    @Container
    static final GenericContainer<?> APICURIO = new GenericContainer<>("apicurio/apicurio-registry:3.3.4")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200));

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("platform.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("platform.kafka.schema-registry-url",
                () -> "http://" + APICURIO.getHost() + ":" + APICURIO.getMappedPort(8080) + "/apis/registry/v3");
        registry.add("platform.kafka.topic-replicas", () -> 1);
        registry.add("platform.kafka.topic-min-insync-replicas", () -> 1);
        registry.add("payment.fault-injection.mid-tx-hold-ms", () -> 0);
    }

    @Autowired
    private PlatformKafkaProperties kafkaProperties;

    @Autowired
    private SimulatedCardGateway cardGateway;

    @Test
    void midKafkaTransactionCrashAbortsUncommittedMessageAndChargesCustomerExactlyOnce() {
        KafkaTemplate<String, Object> ingressProducer =
                new KafkaTemplate<>(KafkaSerdeFactory.idempotentAvroProducerFactory(kafkaProperties));
        String orderId = "ord-eos-crash-" + UUID.randomUUID();

        ConsumerFactory<String, Object> readCommittedFactory = KafkaSerdeFactory.avroConsumerFactory(
                kafkaProperties, "payment-it-verifier-" + UUID.randomUUID());

        try (Consumer<String, Object> consumer = readCommittedFactory.createConsumer()) {
            consumer.subscribe(List.of(
                    TopicNames.PAYMENT_AUTHORIZED,
                    TopicNames.PAYMENT_DECLINED,
                    TopicNames.PAYMENT_REFUNDED));

            OrderCreated crashOnceOrder = OrderCreated.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOrderId(orderId)
                    .setCustomerId("cust-eos")
                    .setCustomerEmail("eos@example.com")
                    .setSku("SKU-HEADSET")
                    .setQuantity(1)
                    .setTotalAmountCents(19900L)
                    .setCurrency("USD")
                    .setPaymentMethodToken(PaymentProcessorService.TOKEN_ABORT_MID_TX_ONCE)
                    .setShippingPostalCode("98101")
                    .setCouponCode(null)
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            ingressProducer.send(TopicNames.ORDER_CREATED, orderId, crashOnceOrder);

            Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                List<GatewayOperationAudit> ops = cardGateway.findOperationsForOrder(orderId);
                assertThat(ops).hasSize(1);
                assertThat(ops.getFirst().executionCount()).isEqualTo(1);
                assertThat(ops.getFirst().deduplicatedRetryCount()).isGreaterThanOrEqualTo(1);
            });

            List<ConsumerRecord<String, Object>> visibleAuthorized =
                    collectRecordsForOrder(consumer, TopicNames.PAYMENT_AUTHORIZED, orderId, Duration.ofSeconds(6));
            assertThat(visibleAuthorized)
                    .as("read_committed consumer must see ONLY the committed retry, never the aborted mid-TX message")
                    .hasSize(1);
            assertThat(visibleAuthorized.getFirst().value()).isInstanceOf(PaymentAuthorized.class);

            // Trigger refund compensation on out-of-stock rejection
            InventoryReservationFailed rejected = new InventoryReservationFailed(
                    UUID.randomUUID().toString(), orderId, "SKU-HEADSET", 1, 0,
                    "Insufficient stock", "eos@example.com", Instant.now().toEpochMilli());
            ingressProducer.send(TopicNames.INVENTORY_REJECTED, orderId, rejected);

            List<ConsumerRecord<String, Object>> refunded =
                    collectRecordsForOrder(consumer, TopicNames.PAYMENT_REFUNDED, orderId, Duration.ofSeconds(10));
            assertThat(refunded).hasSize(1);
            assertThat(refunded.getFirst().value()).isInstanceOf(PaymentRefunded.class);

            // Also verify declined path
            String declinedOrderId = "ord-dec-" + UUID.randomUUID();
            OrderCreated declinedOrder = OrderCreated.newBuilder(crashOnceOrder)
                    .setEventId(UUID.randomUUID().toString())
                    .setOrderId(declinedOrderId)
                    .setPaymentMethodToken("tok_decline_card")
                    .build();
            ingressProducer.send(TopicNames.ORDER_CREATED, declinedOrderId, declinedOrder);

            List<ConsumerRecord<String, Object>> declinedRecords =
                    collectRecordsForOrder(consumer, TopicNames.PAYMENT_DECLINED, declinedOrderId, Duration.ofSeconds(10));
            assertThat(declinedRecords).hasSize(1);
            assertThat(declinedRecords.getFirst().value()).isInstanceOf(PaymentDeclined.class);
        }
    }

    private static List<ConsumerRecord<String, Object>> collectRecordsForOrder(
            Consumer<String, Object> consumer, String topic, String orderId, Duration waitWindow) {
        List<ConsumerRecord<String, Object>> matches = new ArrayList<>();
        Instant deadline = Instant.now().plus(waitWindow);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(350));
            for (ConsumerRecord<String, Object> rec : records.records(topic)) {
                if (orderId.equals(rec.key())) {
                    matches.add(rec);
                }
            }
            if (!matches.isEmpty() && Duration.between(Instant.now(), deadline).toMillis() > 1200) {
                deadline = Instant.now().plusMillis(1200);
            }
        }
        return matches;
    }
}
