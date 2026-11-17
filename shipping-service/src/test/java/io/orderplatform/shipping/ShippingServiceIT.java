package io.orderplatform.shipping;

import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.shipping.domain.ShipmentRepository;
import io.orderplatform.shipping.domain.ShippingBookingService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ShippingServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("shipping_db")
            .withUsername("shipping_user")
            .withPassword("shipping_pass");

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
        registry.add("shipping.retry.initial-delay-ms", () -> 150);
        registry.add("shipping.retry.multiplier", () -> 2.0);
        registry.add("shipping.retry.max-delay-ms", () -> 600);
    }

    @Autowired
    private KafkaTemplate<String, Object> shippingKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, byte[]> shippingByteKafkaTemplate;

    @Autowired
    private PlatformKafkaProperties kafkaProperties;

    @Autowired
    private ShipmentRepository shipmentRepository;

    @Test
    void poisonMessagesRouteThroughRetryTopicsToDltWithoutBlockingSamePartitionAndSucceedOnReplay() {
        ConsumerFactory<String, Object> verifyFactory = KafkaSerdeFactory.avroConsumerFactory(
                kafkaProperties, "shipping-it-verifier-" + UUID.randomUUID());

        try (Consumer<String, Object> consumer = verifyFactory.createConsumer()) {
            String dltTopic = TopicNames.dltTopic(TopicNames.INVENTORY_RESERVED);
            consumer.subscribe(List.of(TopicNames.SHIPPING_SCHEDULED, dltTopic));

            // 1. Send corrupt non-Avro bytes + transient-poison Avro event + healthy order to SAME partition 0
            String corruptKey = "ord-corrupt-" + UUID.randomUUID();
            shippingByteKafkaTemplate.send(new ProducerRecord<>(
                    TopicNames.INVENTORY_RESERVED, 0, corruptKey, "CORRUPT-BYTES".getBytes(StandardCharsets.UTF_8)));

            String poisonOrderId = "ord-poison-" + UUID.randomUUID();
            InventoryReserved poisonEvent = new InventoryReserved(
                    UUID.randomUUID().toString(), poisonOrderId, "res-p", "SKU-HEADSET", 1,
                    ShippingBookingService.POSTAL_POISON_TRANSIENT, "p@b.com", Instant.now().toEpochMilli());
            shippingKafkaTemplate.send(new ProducerRecord<>(
                    TopicNames.INVENTORY_RESERVED, 0, poisonOrderId, poisonEvent));

            String healthyOrderId = "ord-healthy-" + UUID.randomUUID();
            InventoryReserved healthyEvent = new InventoryReserved(
                    UUID.randomUUID().toString(), healthyOrderId, "res-h", "SKU-HEADSET", 1,
                    "98101", "ok@b.com", Instant.now().toEpochMilli());
            shippingKafkaTemplate.send(new ProducerRecord<>(
                    TopicNames.INVENTORY_RESERVED, 0, healthyOrderId, healthyEvent));

            // 2. Partition 0 keeps moving: healthyOrderId is scheduled without being blocked!
            ConsumerRecord<String, Object> healthyScheduled =
                    pollForKey(consumer, TopicNames.SHIPPING_SCHEDULED, healthyOrderId);
            assertThat(healthyScheduled.value()).isInstanceOf(ShipmentCreated.class);

            // 3. Both corrupt message and exhausted retry message land in inventory.reserved.v1.dlt
            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(shipmentRepository.countDeadLettersForKey(corruptKey)).isEqualTo(1);
                assertThat(shipmentRepository.countDeadLettersForKey(poisonOrderId)).isEqualTo(1);
            });

            ConsumerRecord<String, Object> dltPoison = pollForKey(consumer, dltTopic, poisonOrderId);
            assertThat(dltPoison.value()).isInstanceOf(InventoryReserved.class);

            // 4. Replay the DLT record with x-dlq-replayed-at header -> succeeds!
            ProducerRecord<String, Object> replayRecord = new ProducerRecord<>(
                    TopicNames.INVENTORY_RESERVED, 0, poisonOrderId, dltPoison.value());
            replayRecord.headers().add(
                    ShippingBookingService.REPLAYED_HEADER,
                    Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            shippingKafkaTemplate.send(replayRecord);

            ConsumerRecord<String, Object> replayedScheduled =
                    pollForKey(consumer, TopicNames.SHIPPING_SCHEDULED, poisonOrderId);
            assertThat(replayedScheduled.value()).isInstanceOf(ShipmentCreated.class);
        }
    }

    private static ConsumerRecord<String, Object> pollForKey(
            Consumer<String, Object> consumer, String topic, String key) {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(350));
            for (ConsumerRecord<String, Object> rec : records.records(topic)) {
                if (key.equals(rec.key())) {
                    return rec;
                }
            }
        }
        throw new AssertionError("Timed out waiting for " + topic + " key=" + key);
    }
}
