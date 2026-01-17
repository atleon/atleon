package io.atleon.examples.spring.kafka.config;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EmbeddedKafkaInitializer implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Set<String> activeProfiles = Stream.of(environment.getActiveProfiles()).collect(Collectors.toSet());
        if (activeProfiles.contains("kafka") && !activeProfiles.contains("integrationTest")) {
            String bootstrapServers = EmbeddedKafka.startAndGetBootstrapServersConnect();
            environment
                    .getPropertySources()
                    .addFirst(new MapPropertySource("embedded-kafka", createProperties(bootstrapServers)));
        }
    }

    private static Map<String, Object> createProperties(String bootstrapServers) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("vars.kafka.bootstrap.servers", bootstrapServers);
        return properties;
    }
}
