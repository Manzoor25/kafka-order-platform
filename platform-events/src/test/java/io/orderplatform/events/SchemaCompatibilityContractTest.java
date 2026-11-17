package io.orderplatform.events;

import io.orderplatform.events.SchemaBackwardCompatibilityValidator.ValidationReport;
import io.orderplatform.events.avro.OrderCreated;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaCompatibilityContractTest {

    static {
        AvroSecurityConfig.registerTrustedPackages();
    }

    private final SchemaBackwardCompatibilityValidator validator = new SchemaBackwardCompatibilityValidator();
    private final Path mainAvroDir = Path.of("src/main/avro");
    private final Path baselineDir = Path.of("src/test/resources/avro-baseline");

    @Test
    void allMainAvroSchemasMaintainBackwardCompatibilityWithBaseline() throws IOException {
        ValidationReport report = validator.validateDirectory(baselineDir, mainAvroDir);

        assertThat(report.checkedCount()).isEqualTo(11);
        assertThat(report.isCompatible())
                .withFailMessage(() -> "Expected BACKWARD compatibility, failures: " + report.failures())
                .isTrue();
    }

    @Test
    void compatibleEvolutionReadsV1BinaryPayloadUsingDefaultValues() throws IOException {
        Schema v1WriterSchema = new Schema.Parser().parse(
                Files.readString(baselineDir.resolve("OrderCreated.avsc")));

        GenericRecord v1Record = new GenericData.Record(v1WriterSchema);
        v1Record.put("eventId", "evt-100");
        v1Record.put("orderId", "ord-100");
        v1Record.put("customerId", "cust-1");
        v1Record.put("customerEmail", "buyer@example.com");
        v1Record.put("sku", "SKU-KEYBOARD");
        v1Record.put("quantity", 2);
        v1Record.put("totalAmountCents", 15998L);
        v1Record.put("currency", "USD");
        v1Record.put("paymentMethodToken", "tok_visa_ok");
        v1Record.put("shippingPostalCode", "98101");
        v1Record.put("occurredAtEpochMs", 1763330000000L);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(v1WriterSchema).write(v1Record, encoder);
        encoder.flush();

        SpecificDatumReader<OrderCreated> v2Reader = new SpecificDatumReader<>(v1WriterSchema, OrderCreated.getClassSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        OrderCreated decoded = v2Reader.read(null, decoder);

        assertThat(decoded.getOrderId()).isEqualTo("ord-100");
        assertThat(decoded.getTotalAmountCents()).isEqualTo(15998L);
        assertThat(decoded.getCouponCode()).isNull();

        ValidationReport compatibleCandidateReport = validator.validateDirectory(
                mainAvroDir, Path.of("src/test/resources/avro-evolution/compatible"));
        assertThat(compatibleCandidateReport.isCompatible()).isTrue();
    }

    @Test
    void breakingRequiredFieldWithoutDefaultIsRejected() throws IOException {
        ValidationReport report = validator.validateDirectory(
                mainAvroDir, Path.of("src/test/resources/avro-evolution/breaking-required-field"));

        assertThat(report.isCompatible()).isFalse();
        assertThat(report.failures()).hasSize(1);
        assertThat(report.failures().getFirst().schemaFile()).isEqualTo("OrderCreated.avsc");
        assertThat(report.failures().getFirst().details()).contains("READER_FIELD_MISSING_DEFAULT_VALUE");
    }

    @Test
    void breakingFieldTypeChangeIsRejected() throws IOException {
        ValidationReport report = validator.validateDirectory(
                mainAvroDir, Path.of("src/test/resources/avro-evolution/breaking-type-change"));

        assertThat(report.isCompatible()).isFalse();
        assertThat(report.failures()).hasSize(1);
        assertThat(report.failures().getFirst().details()).contains("TYPE_MISMATCH");
    }
}
