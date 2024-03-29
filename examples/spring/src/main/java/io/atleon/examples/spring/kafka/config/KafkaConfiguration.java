package io.atleon.examples.spring.kafka.config;

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
}
