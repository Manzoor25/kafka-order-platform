package io.orderplatform.common.kafka;

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.avro.AvroSerdeConfig;
import io.apicurio.registry.serde.avro.strategy.RecordIdStrategy;
import io.apicurio.registry.serde.config.IdOption;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.orderplatform.events.AvroSecurityConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory methods for idempotent/transactional Avro Kafka producers, KIP-848 manual-commit
 * read_committed consumers, and poison-pill safe DeadLetterPublishingRecoverer error handlers.
 */
public final class KafkaSerdeFactory {

    static {
        AvroSecurityConfig.registerTrustedPackages();
    }

    private KafkaSerdeFactory() {
    }

    public static Map<String, Object> avroProducerConfigs(PlatformKafkaProperties props) {
        AvroSecurityConfig.registerTrustedPackages();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroOrRawBytesSerializer.class);

        // Idempotent producer guarantees: acks=all + enable.idempotence=true
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        // Apicurio Registry v3 Avro 5-byte contentId wire framing (also compatible with Kafbat UI ccompat)
        cfg.put(SchemaResolverConfig.REGISTRY_URL, props.getSchemaRegistryUrl());
        cfg.put(SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, true);
        cfg.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, RecordIdStrategy.class.getName());
        cfg.put(KafkaSerdeConfig.ENABLE_HEADERS, false);
        cfg.put(SerdeConfig.USE_ID, IdOption.contentId.name());
        return cfg;
    }

    public static ProducerFactory<String, Object> idempotentAvroProducerFactory(PlatformKafkaProperties props) {
        return new DefaultKafkaProducerFactory<>(avroProducerConfigs(props));
    }

    public static ProducerFactory<String, Object> transactionalAvroProducerFactory(
            PlatformKafkaProperties props,
            String transactionIdPrefix) {
        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(avroProducerConfigs(props));
        factory.setTransactionIdPrefix(transactionIdPrefix);
        return factory;
    }

    public static KafkaTemplate<String, byte[]> byteArrayKafkaTemplate(PlatformKafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg));
    }

    public static Map<String, Object> avroConsumerConfigs(
            PlatformKafkaProperties props,
            String groupId) {
        AvroSecurityConfig.registerTrustedPackages();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // KIP-848 next-generation consumer group rebalance protocol
        cfg.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, props.getConsumerGroupProtocol());

        // Manual offset commits only + read_committed for transactional producers
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ErrorHandlingDeserializer prevents corrupt non-Avro bytes from wedging a partition
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        cfg.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        cfg.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, AvroOrRawBytesDeserializer.class);

        cfg.put(SchemaResolverConfig.REGISTRY_URL, props.getSchemaRegistryUrl());
        cfg.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, RecordIdStrategy.class.getName());
        cfg.put(KafkaSerdeConfig.ENABLE_HEADERS, false);
        cfg.put(SerdeConfig.USE_ID, IdOption.contentId.name());
        cfg.put(AvroSerdeConfig.USE_SPECIFIC_AVRO_READER, true);
        return cfg;
    }

    public static ConsumerFactory<String, Object> avroConsumerFactory(
            PlatformKafkaProperties props,
            String groupId) {
        return new DefaultKafkaConsumerFactory<>(avroConsumerConfigs(props, groupId));
    }

    /**
     * Builds a DefaultErrorHandler that routes corrupt payloads (DeserializationException) and
     * unrecoverable failures to {@code <original-topic>.dlt} on the matching partition while
     * committing the offset so the partition keeps moving.
     */
    public static DefaultErrorHandler deadLetterErrorHandler(
            KafkaTemplate<String, Object> avroTemplate,
            KafkaTemplate<String, byte[]> byteTemplate) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, byteTemplate);
        templates.put(Object.class, avroTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                templates,
                (record, ex) -> {
                    String baseTopic = record.topic().replaceFirst("\\.retry-\\d+$", "");
                    return new TopicPartition(baseTopic + ".dlt", record.partition());
                });

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(250L, 1L));
        handler.addNotRetryableExceptions(DeserializationException.class, IllegalArgumentException.class);
        handler.setCommitRecovered(true);
        return handler;
    }

    public static ConcurrentKafkaListenerContainerFactory<String, Object> manualAckListenerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
