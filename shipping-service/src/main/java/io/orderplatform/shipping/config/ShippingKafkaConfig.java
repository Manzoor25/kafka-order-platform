package io.orderplatform.shipping.config;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.events.TopicNames;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.List;

/**
 * Configures non-blocking exponential-backoff retry topics
 * ({@code inventory.reserved.v1.retry-0}, {@code .retry-1}, {@code .retry-2}) and dead-letter
 * topic ({@code inventory.reserved.v1.dlt}) mapped onto our Topics-as-Code definitions.
 */
@Configuration
public class ShippingKafkaConfig {

    public static final String CONSUMER_GROUP = "shipping-service-group";

    @Bean
    public KafkaTemplate<String, Object> shippingKafkaTemplate(PlatformKafkaProperties props) {
        KafkaTemplate<String, Object> template =
                new KafkaTemplate<>(KafkaSerdeFactory.idempotentAvroProducerFactory(props));
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public KafkaTemplate<String, byte[]> shippingByteKafkaTemplate(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.byteArrayKafkaTemplate(props);
    }

    @Bean
    public ConsumerFactory<String, Object> shippingConsumerFactory(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.avroConsumerFactory(props, CONSUMER_GROUP);
    }

    @Bean(name = "shippingKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> shippingKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> shippingConsumerFactory,
            KafkaTemplate<String, Object> shippingKafkaTemplate,
            KafkaTemplate<String, byte[]> shippingByteKafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(shippingConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);
        DefaultErrorHandler fallbackErrorHandler = KafkaSerdeFactory.deadLetterErrorHandler(
                shippingKafkaTemplate, shippingByteKafkaTemplate);
        factory.setCommonErrorHandler(fallbackErrorHandler);
        return factory;
    }

    @Bean
    public RetryTopicConfiguration shippingInventoryReservedRetryConfig(
            KafkaTemplate<String, Object> shippingKafkaTemplate,
            ConcurrentKafkaListenerContainerFactory<String, Object> shippingKafkaListenerContainerFactory,
            @Value("${shipping.retry.initial-delay-ms:1000}") long initialDelayMs,
            @Value("${shipping.retry.multiplier:2.0}") double multiplier,
            @Value("${shipping.retry.max-delay-ms:4000}") long maxDelayMs) {
        return RetryTopicConfigurationBuilder.newInstance()
                .exponentialBackoff(initialDelayMs, multiplier, maxDelayMs)
                .maxAttempts(4) // 1 main + 3 retry topics (.retry-0, .retry-1, .retry-2) -> .dlt
                .retryTopicSuffix(".retry")
                .dltSuffix(".dlt")
                .suffixTopicsWithIndexValues()
                .sameIntervalTopicReuseStrategy(SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
                .doNotAutoCreateRetryTopics()
                .includeTopic(TopicNames.INVENTORY_RESERVED)
                .listenerFactory(shippingKafkaListenerContainerFactory)
                .notRetryOn(List.of(DeserializationException.class, IllegalArgumentException.class))
                .traversingCauses(true)
                .dltHandlerMethod("shippingEventListener", "onDeadLetter")
                .dltProcessingFailureStrategy(DltStrategy.FAIL_ON_ERROR)
                .create(shippingKafkaTemplate);
    }

    @Bean
    public ProcessedEventStore shippingProcessedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }

    @Bean
    public Tracer shippingTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        OpenTelemetry otel = openTelemetryProvider.getIfAvailable(GlobalOpenTelemetry::get);
        return otel.getTracer("shipping-service");
    }
}
