package io.atleon.examples.spring.kafka.config;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.util.Collections;
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
            PropertySource<?> propertySource = new MapPropertySource("embedded.kafka", createProperties(bootstrapServers));
            environment.getPropertySources().addLast(propertySource);
        }
    }

    private static Map<String, Object> createProperties(String bootstrapServers) {
        return Collections.singletonMap("example.kafka." + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }
}
