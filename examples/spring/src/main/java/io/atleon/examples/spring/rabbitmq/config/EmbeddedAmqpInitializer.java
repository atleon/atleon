package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EmbeddedAmqpInitializer implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Set<String> activeProfiles = Stream.of(environment.getActiveProfiles()).collect(Collectors.toSet());
        if (activeProfiles.contains("rabbitmq") && !activeProfiles.contains("integrationTest")) {
            EmbeddedAmqpConfig embeddedAmqpConfig = EmbeddedAmqp.start(15672);
            PropertySource<?> propertySource = new MapPropertySource("embedded.amqp", createProperties(embeddedAmqpConfig));
            environment.getPropertySources().addLast(propertySource);
        }
    }

    private static Map<String, Object> createProperties(EmbeddedAmqpConfig config) {
        return config.asMap().entrySet().stream()
            .collect(Collectors.toMap(entry -> "example.rabbitmq." + entry.getKey(), Map.Entry::getValue));
    }
}
