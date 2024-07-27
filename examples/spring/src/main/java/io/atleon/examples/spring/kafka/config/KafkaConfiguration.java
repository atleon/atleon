package io.atleon.examples.spring.kafka.config;

import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Bean("exampleKafkaProperties")
    @ConfigurationProperties(prefix = "example.kafka")
    public Map<String, String> exampleKafkaProperties() {
        return new HashMap<>();
    }

    @Bean("exampleKafkaConfigSource")
    public KafkaConfigSource exampleKafkaConfigSource(@Qualifier("exampleKafkaProperties") Map<String, String> properties) {
        return KafkaConfigSource.useClientIdAsName()
            .withAll(properties)
            .with(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
    }
}
