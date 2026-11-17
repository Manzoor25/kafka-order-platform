package io.orderplatform.order.config;

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
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Configuration
public class OrderKafkaConfig {

    public static final String CONSUMER_GROUP = "order-service-group";

    @Bean
    public ProducerFactory<String, Object> orderProducerFactory(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.idempotentAvroProducerFactory(props);
    }

    @Bean
    public KafkaTemplate<String, Object> orderKafkaTemplate(ProducerFactory<String, Object> orderProducerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(orderProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public KafkaTemplate<String, byte[]> orderByteArrayKafkaTemplate(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.byteArrayKafkaTemplate(props);
    }

    @Bean
    public ConsumerFactory<String, Object> orderConsumerFactory(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.avroConsumerFactory(props, CONSUMER_GROUP);
    }

    @Bean
    public DefaultErrorHandler orderKafkaErrorHandler(
            KafkaTemplate<String, Object> orderKafkaTemplate,
            KafkaTemplate<String, byte[]> orderByteArrayKafkaTemplate) {
        return KafkaSerdeFactory.deadLetterErrorHandler(orderKafkaTemplate, orderByteArrayKafkaTemplate);
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> orderConsumerFactory,
            DefaultErrorHandler orderKafkaErrorHandler) {
        return KafkaSerdeFactory.manualAckListenerFactory(orderConsumerFactory, orderKafkaErrorHandler);
    }

    @Bean
    public ProcessedEventStore processedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }

    @Bean
    public Tracer orderTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        OpenTelemetry otel = openTelemetryProvider.getIfAvailable(GlobalOpenTelemetry::get);
        return otel.getTracer("order-service");
    }
}
