# ADR-0004: Apicurio Registry 3.3.4 with Apache Avro 1.12.2 and BACKWARD Compatibility

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
Event-driven architectures without strict schema governance inevitably suffer silent consumer deserialization breakages when producers rename fields, change types, or introduce required fields. We require compact binary serialization, strong Java 21 code generation, Apache-2.0 open-source licensing, and automated CI enforcement.

## Decision
1. Define all 11 event contracts as **Apache Avro 1.12.2** `.avsc` schemas in `platform-events`, generating strongly typed `SpecificRecord` classes at build time.
2. Run **Apicurio Registry 3.3.4** (`apicurio-registry-avro-serde-kafka`), exposing both the native v3 API (`/apis/registry/v3`) for microservices and the Confluent-compatible v7 API (`/apis/ccompat/v7`) for Kafbat Kafka UI inspection.
3. Enforce **`BACKWARD` compatibility** (new reader schema can read payloads written with the previous writer schema) in two layers:
   - **CI Gate:** `SchemaBackwardCompatibilityValidator` compares every candidate `.avsc` in `src/main/avro` against `src/test/resources/avro-baseline` during `mvn test` / CI and fails the build on incompatible evolutions.
   - **Runtime Registry Rule:** Apicurio global and artifact-level `COMPATIBILITY = BACKWARD` rules reject breaking schema registrations with HTTP `409 RuleViolationException`.

## Consequences
- **Positive:** Consumers can be upgraded **before** producers during rolling deployments without downtime. Adding optional fields with `"default": null` (e.g., `couponCode` or `loyaltyTier`) succeeds seamlessly, while breaking changes are caught in milliseconds during CI before merge.
- **Negative:** Removing or renaming a field in-place requires a multi-step deprecation or a new major topic version (`.v2`).
