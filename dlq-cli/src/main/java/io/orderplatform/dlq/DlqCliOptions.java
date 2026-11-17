package io.orderplatform.dlq;

import java.util.HashMap;
import java.util.Map;

public record DlqCliOptions(
        String command,
        String topic,
        String targetTopic,
        String bootstrapServers,
        String schemaRegistryUrl,
        String keyFilter,
        Integer partitionFilter,
        Long offsetFilter,
        String overridePostalCode,
        boolean replayRawCorrupt,
        boolean dryRun,
        String format
) {

    public static DlqCliOptions parse(String[] args) {
        if (args == null || args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            throw new IllegalArgumentException(usage());
        }
        String command = args[0].trim().toLowerCase();
        if (!"inspect".equals(command) && !"replay".equals(command)) {
            throw new IllegalArgumentException("Unknown subcommand '" + command + "'.\n" + usage());
        }

        Map<String, String> flags = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            String token = args[i];
            if (!token.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument '" + token + "'.\n" + usage());
            }
            String flagName = token.substring(2);
            if ("dry-run".equals(flagName) || "replay-raw".equals(flagName)) {
                flags.put(flagName, "true");
            } else if (flagName.contains("=")) {
                int idx = flagName.indexOf('=');
                flags.put(flagName.substring(0, idx), flagName.substring(idx + 1));
            } else {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for --" + flagName);
                }
                flags.put(flagName, args[++i]);
            }
        }

        String topic = flags.get("topic");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("--topic is required.\n" + usage());
        }

        String defaultBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String defaultRegistry = System.getenv().getOrDefault(
                "SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/registry/v3");

        return new DlqCliOptions(
                command,
                topic.trim(),
                flags.get("target-topic"),
                flags.getOrDefault("bootstrap-servers", defaultBootstrap),
                flags.getOrDefault("schema-registry-url", defaultRegistry),
                flags.get("key"),
                flags.containsKey("partition") ? Integer.parseInt(flags.get("partition")) : null,
                flags.containsKey("offset") ? Long.parseLong(flags.get("offset")) : null,
                flags.get("override-postal-code"),
                Boolean.parseBoolean(flags.getOrDefault("replay-raw", "false")),
                Boolean.parseBoolean(flags.getOrDefault("dry-run", "false")),
                flags.getOrDefault("format", "table").toLowerCase()
        );
    }

    public static String usage() {
        return """
                Usage:
                  dlq-cli inspect --topic <dlt-topic> [--key <orderId>] [--format table|json]
                                  [--bootstrap-servers host:9092] [--schema-registry-url http://host:8081/apis/registry/v3]
                  dlq-cli replay  --topic <dlt-topic> [--target-topic <topic>] [--key <orderId>]
                                  [--partition <n> --offset <m>] [--override-postal-code <zip>]
                                  [--replay-raw] [--dry-run]
                """;
    }
}
