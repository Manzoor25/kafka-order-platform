package io.orderplatform.events;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Enforces Avro BACKWARD compatibility (new reader schema must read records written with prior writer schema).
 * Runnable both from JUnit contract tests and as a standalone CI gate CLI.
 */
public final class SchemaBackwardCompatibilityValidator {

    public record ValidationFailure(String schemaFile, String details) {
    }

    public record ValidationReport(int checkedCount, List<ValidationFailure> failures) {
        public boolean isCompatible() {
            return failures.isEmpty();
        }
    }

    public ValidationReport validateDirectory(Path baselineDir, Path candidateDir) throws IOException {
        if (!Files.isDirectory(baselineDir)) {
            throw new IllegalArgumentException("Baseline directory does not exist: " + baselineDir);
        }
        if (!Files.isDirectory(candidateDir)) {
            throw new IllegalArgumentException("Candidate directory does not exist: " + candidateDir);
        }

        List<Path> candidateFiles;
        try (Stream<Path> stream = Files.list(candidateDir)) {
            candidateFiles = stream
                    .filter(p -> p.getFileName().toString().endsWith(".avsc"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        }

        List<ValidationFailure> failures = new ArrayList<>();
        int checked = 0;

        for (Path candidateFile : candidateFiles) {
            String fileName = candidateFile.getFileName().toString();
            Path baselineFile = baselineDir.resolve(fileName);
            Schema candidateSchema = new Schema.Parser().parse(Files.readString(candidateFile));
            if (!Files.exists(baselineFile)) {
                // Brand-new event schema has no prior writer version to break
                checked++;
                continue;
            }
            Schema baselineSchema = new Schema.Parser().parse(Files.readString(baselineFile));
            SchemaPairCompatibility result = checkBackward(baselineSchema, candidateSchema);
            checked++;
            if (result.getType() != SchemaCompatibilityType.COMPATIBLE) {
                String details = result.getResult().getIncompatibilities().stream()
                        .map(inc -> inc.getType() + " at " + inc.getLocation() + ": " + inc.getMessage())
                        .reduce((a, b) -> a + "; " + b)
                        .orElse(result.getDescription());
                failures.add(new ValidationFailure(fileName, details));
            }
        }
        return new ValidationReport(checked, List.copyOf(failures));
    }

    public SchemaPairCompatibility checkBackward(Schema previousWriterSchema, Schema newReaderSchema) {
        return SchemaCompatibility.checkReaderWriterCompatibility(newReaderSchema, previousWriterSchema);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: SchemaBackwardCompatibilityValidator <baseline-avro-dir> <candidate-avro-dir>");
            System.exit(2);
        }
        Path baselineDir = Path.of(args[0]);
        Path candidateDir = Path.of(args[1]);
        SchemaBackwardCompatibilityValidator validator = new SchemaBackwardCompatibilityValidator();
        ValidationReport report = validator.validateDirectory(baselineDir, candidateDir);

        System.out.printf("Checked %d Avro schema(s) against baseline [%s] using BACKWARD compatibility.%n",
                report.checkedCount(), baselineDir);
        if (report.isCompatible()) {
            System.out.println("[PASS] All candidate schemas are BACKWARD compatible.");
            return;
        }
        for (ValidationFailure failure : report.failures()) {
            System.err.printf("[REJECTED] %s -> %s%n", failure.schemaFile(), failure.details());
        }
        System.exit(1);
    }
}
