package io.orderplatform.order.outbox;

import io.orderplatform.events.AvroSecurityConfig;
import io.orderplatform.events.avro.OrderCancelled;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.OrderCreated;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Deterministic Avro-JSON serializer/deserializer for storing SpecificRecord events inside
 * PostgreSQL {@code outbox_events} before publishing binary Avro records to Kafka.
 */
public final class AvroJsonCodec {

    static {
        AvroSecurityConfig.registerTrustedPackages();
    }

    private AvroJsonCodec() {
    }

    public static <T extends SpecificRecord> String toJson(T record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            new SpecificDatumWriter<T>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode Avro record to JSON for outbox", ex);
        }
    }

    public static SpecificRecord fromJson(String eventType, String json) {
        Schema schema = switch (eventType) {
            case "OrderCreated" -> OrderCreated.getClassSchema();
            case "OrderConfirmed" -> OrderConfirmed.getClassSchema();
            case "OrderCancelled" -> OrderCancelled.getClassSchema();
            default -> throw new IllegalArgumentException("Unsupported outbox eventType: " + eventType);
        };
        try {
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            SpecificDatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema);
            return reader.read(null, decoder);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode outbox Avro JSON for " + eventType, ex);
        }
    }
}
