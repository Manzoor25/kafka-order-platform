package io.orderplatform.common.kafka;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Delegates Avro records to Apicurio's {@link AvroKafkaSerializer} while passing raw
 * {@code byte[]} payloads straight through when {@code DeadLetterPublishingRecoverer}
 * forwards corrupt un-deserializable poison bytes to a {@code .dlt} topic.
 */
public class AvroOrRawBytesSerializer implements Serializer<Object> {

    private final AvroKafkaSerializer avroDelegate = new AvroKafkaSerializer();
    private final ByteArraySerializer byteDelegate = new ByteArraySerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        avroDelegate.configure(configs, isKey);
        byteDelegate.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[] rawBytes) {
            return byteDelegate.serialize(topic, rawBytes);
        }
        return avroDelegate.serialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[] rawBytes) {
            return byteDelegate.serialize(topic, headers, rawBytes);
        }
        return avroDelegate.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        avroDelegate.close();
        byteDelegate.close();
    }
}
