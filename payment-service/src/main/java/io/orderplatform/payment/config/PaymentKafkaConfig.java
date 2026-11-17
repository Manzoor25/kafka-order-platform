package io.orderplatform.payment.config;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import javax.sql.DataSource;

/**
 * Configures Kafka transactional read-process-write (EOS) for payment-service:
 * {@code KafkaTransactionManager} binds outbound {@code PaymentAuthorized}/{@code PaymentDeclined}/
 * {@code PaymentRefunded} writes and consumer offset commits ({@code sendOffsetsToTransaction})
 * into one atomic Kafka transaction.
 */
@Configuration
public class PaymentKafkaConfig {

    public static final String CONSUMER_GROUP = "payment-service-group";

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public ProducerFactory<String, Object> paymentTransactionalProducerFactory(
            PlatformKafkaProperties props,
            @Value("${payment.kafka.transaction-id-prefix:payment-eos-tx-}") String txPrefix) {
        return KafkaSerdeFactory.transactionalAvroProducerFactory(props, txPrefix);
    }

    @Bean
    public KafkaTransactionManager<String, Object> paymentKafkaTransactionManager(
            ProducerFactory<String, Object> paymentTransactionalProducerFactory) {
        return new KafkaTransactionManager<>(paymentTransactionalProducerFactory);
    }

    @Bean
    public KafkaTemplate<String, Object> paymentTransactionalKafkaTemplate(
            ProducerFactory<String, Object> paymentTransactionalProducerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(paymentTransactionalProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public ConsumerFactory<String, Object> paymentConsumerFactory(PlatformKafkaProperties props) {
        return KafkaSerdeFactory.avroConsumerFactory(props, CONSUMER_GROUP);
    }

    @Bean(name = "eosKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> eosKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> paymentConsumerFactory,
            KafkaTransactionManager<String, Object> paymentKafkaTransactionManager,
            KafkaTemplate<String, Object> paymentTransactionalKafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentConsumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(paymentKafkaTransactionManager);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setObservationEnabled(true);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                paymentTransactionalKafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
        DefaultAfterRollbackProcessor<String, Object> rollbackProcessor =
                new DefaultAfterRollbackProcessor<>(
                        recoverer,
                        new FixedBackOff(300L, 2L),
                        paymentTransactionalKafkaTemplate,
                        true);
        rollbackProcessor.addNotRetryableExceptions(DeserializationException.class, IllegalArgumentException.class);
        factory.setAfterRollbackProcessor(rollbackProcessor);
        return factory;
    }

    @Bean
    public ProcessedEventStore paymentProcessedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }

    @Bean
    public Tracer paymentTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        OpenTelemetry otel = openTelemetryProvider.getIfAvailable(GlobalOpenTelemetry::get);
        return otel.getTracer("payment-service");
    }
}
