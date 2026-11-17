package io.orderplatform.common.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "platform.kafka")
public class PlatformKafkaProperties {

    private String bootstrapServers = "localhost:9092";
    private String schemaRegistryUrl = "http://localhost:8081/apis/registry/v3";
    private boolean enforceBackwardRuleOnStartup = false;
    private int topicPartitions = 6;
    private int topicReplicas = 3;
    private int topicMinInsyncReplicas = 2;
    private String consumerGroupProtocol = "consumer";
    private long retryInitialIntervalMs = 1000L;
    private double retryMultiplier = 2.0;
    private int retryTopicsCount = 3;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public boolean isEnforceBackwardRuleOnStartup() {
        return enforceBackwardRuleOnStartup;
    }

    public void setEnforceBackwardRuleOnStartup(boolean enforceBackwardRuleOnStartup) {
        this.enforceBackwardRuleOnStartup = enforceBackwardRuleOnStartup;
    }

    public int getTopicPartitions() {
        return topicPartitions;
    }

    public void setTopicPartitions(int topicPartitions) {
        this.topicPartitions = topicPartitions;
    }

    public int getTopicReplicas() {
        return topicReplicas;
    }

    public void setTopicReplicas(int topicReplicas) {
        this.topicReplicas = topicReplicas;
    }

    public int getTopicMinInsyncReplicas() {
        return topicMinInsyncReplicas;
    }

    public void setTopicMinInsyncReplicas(int topicMinInsyncReplicas) {
        this.topicMinInsyncReplicas = topicMinInsyncReplicas;
    }

    public String getConsumerGroupProtocol() {
        return consumerGroupProtocol;
    }

    public void setConsumerGroupProtocol(String consumerGroupProtocol) {
        this.consumerGroupProtocol = consumerGroupProtocol;
    }

    public long getRetryInitialIntervalMs() {
        return retryInitialIntervalMs;
    }

    public void setRetryInitialIntervalMs(long retryInitialIntervalMs) {
        this.retryInitialIntervalMs = retryInitialIntervalMs;
    }

    public double getRetryMultiplier() {
        return retryMultiplier;
    }

    public void setRetryMultiplier(double retryMultiplier) {
        this.retryMultiplier = retryMultiplier;
    }

    public int getRetryTopicsCount() {
        return retryTopicsCount;
    }

    public void setRetryTopicsCount(int retryTopicsCount) {
        this.retryTopicsCount = retryTopicsCount;
    }
}
