package io.orderplatform.notification.config;

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
public class NotificationKafkaConfig {

    public static final String CONSUMER_GROUP = "notification-service-group";

    @Bean
    public KafkaTemplate<String, Object> notificationKafkaTemplate(PlatformKafkaProperties props) {
        return new KafkaTemplate<>(KafkaSerdeFactory.idempotentAvroProducerFactory(props));
    }

    @Bean
    public KafkaTemplate<String, byte[]> notificationByteKafkaTemplate(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.byteArrayKafkaTemplate(props);
    }

    @Bean
    public ConsumerFactory<String, Object> notificationConsumerFactory(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.avroConsumerFactory(props, CONSUMER_GROUP);
    }

    @Bean(name = "notificationKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> notificationKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> notificationConsumerFactory,
            KafkaTemplate<String, Object> notificationKafkaTemplate,
            KafkaTemplate<String, byte[]> notificationByteKafkaTemplate) {
        DefaultErrorHandler errorHandler = KafkaSerdeFactory.deadLetterErrorHandler(
                notificationKafkaTemplate, notificationByteKafkaTemplate);
        return KafkaSerdeFactory.manualAckListenerFactory(notificationConsumerFactory, errorHandler);
    }

    @Bean
    public ProcessedEventStore notificationProcessedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }

    @Bean
    public Tracer notificationTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        OpenTelemetry otel = openTelemetryProvider.getIfAvailable(GlobalOpenTelemetry::get);
        return otel.getTracer("notification-service");
    }
}
