package io.orderplatform.inventory;

import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.inventory.domain.InventoryStockRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class InventoryServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("inventory_db")
            .withUsername("inventory_user")
            .withPassword("inventory_pass");

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
        registry.add("management.tracing.export.otlp.enabled", () -> false);
    }

    @Autowired
    private KafkaTemplate<String, Object> inventoryKafkaTemplate;

    @Autowired
    private PlatformKafkaProperties kafkaProperties;

    @Autowired
    private InventoryStockRepository stockRepository;

    @Test
    void reservesAndReleasesStockOnShippingFailureAndRejectsOutOfStockSkus() {
        ConsumerFactory<String, Object> verifyFactory = KafkaSerdeFactory.avroConsumerFactory(
                kafkaProperties, "inventory-it-verifier-" + UUID.randomUUID());

        try (Consumer<String, Object> consumer = verifyFactory.createConsumer()) {
            consumer.subscribe(List.of(
                    TopicNames.INVENTORY_RESERVED,
                    TopicNames.INVENTORY_REJECTED,
                    TopicNames.INVENTORY_RELEASED));

            int initialLimitedQty = stockRepository.findStockBySku("SKU-LIMITED-1").orElseThrow().availableQty();
            assertThat(initialLimitedQty).isEqualTo(1);

            String order1 = "ord-inv-ok-" + UUID.randomUUID();
            inventoryKafkaTemplate.send(TopicNames.PAYMENT_AUTHORIZED, order1, new PaymentAuthorized(
                    UUID.randomUUID().toString(), order1, "pay-1", "ch_1",
                    "SKU-LIMITED-1", 1, 9900L, "USD", "98101", "a@b.com", Instant.now().toEpochMilli()));

            ConsumerRecord<String, Object> reservedRec = pollForOrder(consumer, TopicNames.INVENTORY_RESERVED, order1);
            assertThat(reservedRec.value()).isInstanceOf(InventoryReserved.class);
            assertThat(stockRepository.findStockBySku("SKU-LIMITED-1").orElseThrow().availableQty()).isZero();

            // Second order for SKU-LIMITED-1 must fail with InventoryReservationFailed
            String order2 = "ord-inv-oos-" + UUID.randomUUID();
            inventoryKafkaTemplate.send(TopicNames.PAYMENT_AUTHORIZED, order2, new PaymentAuthorized(
                    UUID.randomUUID().toString(), order2, "pay-2", "ch_2",
                    "SKU-LIMITED-1", 1, 9900L, "USD", "98101", "a@b.com", Instant.now().toEpochMilli()));

            ConsumerRecord<String, Object> rejectedRec = pollForOrder(consumer, TopicNames.INVENTORY_REJECTED, order2);
            assertThat(rejectedRec.value()).isInstanceOf(InventoryReservationFailed.class);

            // Compensating ShipmentFailed for order1 restores SKU-LIMITED-1 back to 1
            inventoryKafkaTemplate.send(TopicNames.SHIPPING_FAILED, order1, new ShipmentFailed(
                    UUID.randomUUID().toString(), order1, "SKU-LIMITED-1", 1,
                    "FAIL-SHIP", "Destination embargo", "a@b.com", Instant.now().toEpochMilli()));

            ConsumerRecord<String, Object> releasedRec = pollForOrder(consumer, TopicNames.INVENTORY_RELEASED, order1);
            assertThat(releasedRec.value()).isInstanceOf(InventoryReleased.class);
            assertThat(stockRepository.findStockBySku("SKU-LIMITED-1").orElseThrow().availableQty()).isEqualTo(1);
        }
    }

    private static ConsumerRecord<String, Object> pollForOrder(
            Consumer<String, Object> consumer, String topic, String orderId) {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(350));
            for (ConsumerRecord<String, Object> rec : records.records(topic)) {
                if (orderId.equals(rec.key())) {
                    return rec;
                }
            }
        }
        throw new AssertionError("Timed out waiting for " + topic + " key=" + orderId);
    }
}
