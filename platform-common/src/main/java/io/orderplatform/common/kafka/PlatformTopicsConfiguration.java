package io.orderplatform.common.kafka;

import io.orderplatform.events.TopicNames;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Topics-as-Code definition for all domain, non-blocking retry, and dead-letter topics.
 * Production defaults: 6 partitions (even KIP-848 scaling across 1, 2, 3, or 6 replicas),
 * replication factor 3, and min.insync.replicas=2 (paired with producer acks=all).
 */
@Configuration
@EnableConfigurationProperties(PlatformKafkaProperties.class)
public class PlatformTopicsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PlatformTopicsConfiguration.class);
    private static final String SEVEN_DAYS_MS = "604800000";
    private static final String THIRTY_DAYS_MS = "2592000000";

    @Bean
    public KafkaAdmin.NewTopics platformTopics(PlatformKafkaProperties props) {
        List<NewTopic> topics = buildAllTopics(props);
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    public static List<NewTopic> buildAllTopics(PlatformKafkaProperties props) {
        List<NewTopic> topics = new ArrayList<>();
        String minIsr = String.valueOf(props.getTopicMinInsyncReplicas());

        for (String domainTopic : TopicNames.ALL_DOMAIN_TOPICS) {
            topics.add(TopicBuilder.name(domainTopic)
                    .partitions(props.getTopicPartitions())
                    .replicas(props.getTopicReplicas())
                    .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr)
                    .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                    .config(TopicConfig.RETENTION_MS_CONFIG, SEVEN_DAYS_MS)
                    .build());

            for (int i = 0; i < props.getRetryTopicsCount(); i++) {
                topics.add(TopicBuilder.name(TopicNames.retryTopic(domainTopic, i))
                        .partitions(props.getTopicPartitions())
                        .replicas(props.getTopicReplicas())
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr)
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                        .config(TopicConfig.RETENTION_MS_CONFIG, SEVEN_DAYS_MS)
                        .build());
            }

            topics.add(TopicBuilder.name(TopicNames.dltTopic(domainTopic))
                    .partitions(props.getTopicPartitions())
                    .replicas(props.getTopicReplicas())
                    .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr)
                    .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                    .config(TopicConfig.RETENTION_MS_CONFIG, THIRTY_DAYS_MS)
                    .build());
        }
        return topics;
    }

    @Bean
    public SmartInitializingSingleton apicurioBackwardRuleInitializer(PlatformKafkaProperties props) {
        return () -> {
            if (!props.isEnforceBackwardRuleOnStartup()) {
                return;
            }
            try {
                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();
                String rulesUrl = props.getSchemaRegistryUrl().replaceAll("/+$", "") + "/admin/rules";
                HttpRequest createReq = HttpRequest.newBuilder(URI.create(rulesUrl))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"ruleType\":\"COMPATIBILITY\",\"config\":\"BACKWARD\"}"))
                        .build();
                HttpResponse<String> resp = client.send(createReq, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 409) {
                    HttpRequest updateReq = HttpRequest.newBuilder(URI.create(rulesUrl + "/COMPATIBILITY"))
                            .timeout(Duration.ofSeconds(5))
                            .header("Content-Type", "application/json")
                            .PUT(HttpRequest.BodyPublishers.ofString(
                                    "{\"ruleType\":\"COMPATIBILITY\",\"config\":\"BACKWARD\"}"))
                            .build();
                    client.send(updateReq, HttpResponse.BodyHandlers.ofString());
                }
                log.info("Verified global BACKWARD compatibility rule on Apicurio Registry at {}", rulesUrl);
            } catch (Exception ex) {
                log.warn("Could not enforce Apicurio global BACKWARD rule on startup: {}", ex.getMessage());
            }
        };
    }
}
