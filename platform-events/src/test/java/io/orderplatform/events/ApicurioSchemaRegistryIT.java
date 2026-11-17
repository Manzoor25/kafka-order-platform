package io.orderplatform.events;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ApicurioSchemaRegistryIT {

    @Container
    static final GenericContainer<?> APICURIO = new GenericContainer<>("apicurio/apicurio-registry:3.3.4")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200));

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Test
    void apicurioEnforcesBackwardCompatibilityAcceptingOptionalFieldAndRejectingBreakingChange() throws Exception {
        String baseUrl = "http://" + APICURIO.getHost() + ":" + APICURIO.getMappedPort(8080) + "/apis/registry/v3";

        // 1. Enable global BACKWARD compatibility rule
        HttpResponse<String> ruleResp = postJson(baseUrl + "/admin/rules",
                "{\"ruleType\":\"COMPATIBILITY\",\"config\":\"BACKWARD\"}");
        assertThat(ruleResp.statusCode()).isIn(200, 204);

        // 2. Register baseline v1 OrderCreated schema
        String v1Schema = Files.readString(Path.of("src/test/resources/avro-baseline/OrderCreated.avsc"));
        String createArtifactPayload = """
                {
                  "artifactId": "order.created.v1-value",
                  "artifactType": "AVRO",
                  "firstVersion": {
                    "version": "1",
                    "content": {
                      "content": %s,
                      "contentType": "application/json"
                    }
                  }
                }
                """.formatted(toJsonStringLiteral(v1Schema));
        HttpResponse<String> v1Resp = postJson(baseUrl + "/groups/default/artifacts", createArtifactPayload);
        assertThat(v1Resp.statusCode()).isEqualTo(200);

        // 3. Evolve with BACKWARD-compatible v2 schema (adds optional couponCode with default null) -> ACCEPTED
        String v2Compatible = Files.readString(Path.of("src/main/avro/OrderCreated.avsc"));
        String newVersionPayload = """
                {
                  "version": "2",
                  "content": {
                    "content": %s,
                    "contentType": "application/json"
                  }
                }
                """.formatted(toJsonStringLiteral(v2Compatible));
        HttpResponse<String> v2Resp = postJson(
                baseUrl + "/groups/default/artifacts/order.created.v1-value/versions",
                newVersionPayload);
        assertThat(v2Resp.statusCode()).isEqualTo(200);

        // 4. Attempt BREAKING v3 schema (required loyaltyTier without default) -> REJECTED (409 RuleViolationException)
        String v3Breaking = Files.readString(
                Path.of("src/test/resources/avro-evolution/breaking-required-field/OrderCreated.avsc"));
        String breakingPayload = """
                {
                  "version": "3",
                  "content": {
                    "content": %s,
                    "contentType": "application/json"
                  }
                }
                """.formatted(toJsonStringLiteral(v3Breaking));
        HttpResponse<String> v3Resp = postJson(
                baseUrl + "/groups/default/artifacts/order.created.v1-value/versions",
                breakingPayload);
        assertThat(v3Resp.statusCode())
                .as("Apicurio v3 breaking-schema rejection response: %s", v3Resp.body())
                .isIn(400, 409, 422);
        assertThat(v3Resp.body()).containsIgnoringCase("RuleViolation");
    }

    private HttpResponse<String> postJson(String url, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static String toJsonStringLiteral(String raw) {
        return "\"" + raw
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "")
                .replace("\n", "\\n") + "\"";
    }
}
