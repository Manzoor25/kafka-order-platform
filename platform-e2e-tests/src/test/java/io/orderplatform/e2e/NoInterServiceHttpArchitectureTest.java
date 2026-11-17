package io.orderplatform.e2e;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Architectural guardrail enforcing the core constraint:
 * "Five services (order, payment, inventory, shipping, notification) communicate only through
 * Kafka events. No service calls another over HTTP."
 */
class NoInterServiceHttpArchitectureTest {

    private static final List<String> SERVICE_MODULES = List.of(
            "order-service",
            "payment-service",
            "inventory-service",
            "shipping-service",
            "notification-service"
    );

    private static final List<String> FORBIDDEN_HTTP_CLIENTS = List.of(
            "org.springframework.web.client.RestClient",
            "org.springframework.web.client.RestTemplate",
            "org.springframework.web.reactive.function.client.WebClient",
            "java.net.http.HttpClient",
            "org.springframework.cloud.openfeign.FeignClient",
            "okhttp3.OkHttpClient"
    );

    @Test
    void noMicroserviceContainsOutboundHttpClientCallsToAnotherService() throws IOException {
        Path repoRoot = resolveRepoRoot();
        List<String> violations = new ArrayList<>();
        int scannedFiles = 0;

        for (String module : SERVICE_MODULES) {
            Path mainSrc = repoRoot.resolve(module).resolve("src").resolve("main").resolve("java");
            assertThat(mainSrc).exists();
            try (Stream<Path> walk = Files.walk(mainSrc)) {
                for (Path file : walk.filter(p -> p.toString().endsWith(".java")).toList()) {
                    scannedFiles++;
                    String source = Files.readString(file);
                    for (String forbidden : FORBIDDEN_HTTP_CLIENTS) {
                        if (source.contains(forbidden)) {
                            violations.add(module + ": " + repoRoot.relativize(file) + " references " + forbidden);
                        }
                    }
                }
            }
        }

        assertThat(scannedFiles).isGreaterThanOrEqualTo(30);
        assertThat(violations)
                .as("Services must communicate exclusively via Kafka events with zero outbound HTTP clients")
                .isEmpty();
    }

    private static Path resolveRepoRoot() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.exists(cwd.resolve("order-service"))) {
            return cwd;
        }
        if (cwd.getParent() != null && Files.exists(cwd.getParent().resolve("order-service"))) {
            return cwd.getParent();
        }
        throw new IllegalStateException("Cannot locate repository root from " + cwd);
    }
}
