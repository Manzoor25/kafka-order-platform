package io.orderplatform.common.kafka;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Delegates Avro deserialization to Apicurio's {@link AvroKafkaDeserializer} on primary and
 * {@code .retry-*} topics so corrupt payloads trigger {@code ErrorHandlingDeserializer} and
 * route straight to {@code <topic>.dlt}. On {@code .dlt} topics, gracefully falls back to
 * returning the raw {@code byte[]} when a dead-lettered record is un-deserializable non-Avro
 * bytes so DLT observers and inspectors never double-fault.
 */
public class AvroOrRawBytesDeserializer implements Deserializer<Object> {

    private final AvroKafkaDeserializer<Object> avroDelegate = new AvroKafkaDeserializer<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        avroDelegate.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (isDltTopic(topic)) {
            try {
                return avroDelegate.deserialize(topic, data);
            } catch (RuntimeException ex) {
                return data;
            }
        }
        return avroDelegate.deserialize(topic, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }
        if (isDltTopic(topic)) {
            try {
                return avroDelegate.deserialize(topic, headers, data);
            } catch (RuntimeException ex) {
                return data;
            }
        }
        return avroDelegate.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        avroDelegate.close();
    }

    private static boolean isDltTopic(String topic) {
        return topic != null && topic.endsWith(".dlt");
    }
}
