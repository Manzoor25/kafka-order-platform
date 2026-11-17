# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:21.0.13_9-jdk-alpine AS workspace-builder
WORKDIR /workspace

COPY mvnw ./
COPY .mvn ./.mvn
COPY pom.xml ./
COPY platform-events ./platform-events
COPY platform-common ./platform-common
COPY order-service ./order-service
COPY payment-service ./payment-service
COPY inventory-service ./inventory-service
COPY shipping-service ./shipping-service
COPY notification-service ./notification-service
COPY dlq-cli ./dlq-cli
COPY platform-e2e-tests ./platform-e2e-tests

RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && \
    ./mvnw -B -ntp package -DskipTests -pl !platform-e2e-tests

FROM eclipse-temurin:21.0.13_9-jre-alpine AS runtime
ARG SERVICE_MODULE
RUN apk add --no-cache curl && \
    addgroup -S platform && adduser -S platform -G platform

WORKDIR /app
COPY --from=workspace-builder /workspace/${SERVICE_MODULE}/target/${SERVICE_MODULE}-1.0.0-exec.jar /app/app.jar

ENV JAVA_TOOL_OPTIONS="-Dorg.apache.avro.SERIALIZABLE_PACKAGES=io.orderplatform.events.avro -XX:MaxRAMPercentage=75.0"

USER platform
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
