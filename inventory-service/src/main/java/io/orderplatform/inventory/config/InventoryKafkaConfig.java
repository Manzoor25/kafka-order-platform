package io.orderplatform.inventory.config;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Configuration
public class InventoryKafkaConfig {

    public static final String CONSUMER_GROUP = "inventory-service-group";

    @Bean
    public KafkaTemplate<String, Object> inventoryKafkaTemplate(PlatformKafkaProperties props) {
        KafkaTemplate<String, Object> template =
                new KafkaTemplate<>(KafkaSerdeFactory.idempotentAvroProducerFactory(props));
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public KafkaTemplate<String, byte[]> inventoryByteKafkaTemplate(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.byteArrayKafkaTemplate(props);
    }

    @Bean
    public ConsumerFactory<String, Object> inventoryConsumerFactory(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.avroConsumerFactory(props, CONSUMER_GROUP);
    }

    @Bean(name = "inventoryKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> inventoryKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> inventoryConsumerFactory,
            KafkaTemplate<String, Object> inventoryKafkaTemplate,
            KafkaTemplate<String, byte[]> inventoryByteKafkaTemplate) {
        DefaultErrorHandler errorHandler = KafkaSerdeFactory.deadLetterErrorHandler(
                inventoryKafkaTemplate, inventoryByteKafkaTemplate);
        return KafkaSerdeFactory.manualAckListenerFactory(inventoryConsumerFactory, errorHandler);
    }

    @Bean
    public ProcessedEventStore inventoryProcessedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }

    @Bean
    public Tracer inventoryTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        OpenTelemetry otel = openTelemetryProvider.getIfAvailable(GlobalOpenTelemetry::get);
        return otel.getTracer("inventory-service");
    }
}
