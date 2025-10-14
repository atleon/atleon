package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
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
            environment.getPropertySources()
                .addFirst(new MapPropertySource("embedded-amqp", createProperties(embeddedAmqpConfig)));
        }
    }

    private static Map<String, Object> createProperties(EmbeddedAmqpConfig config) {
        Map<String, Object> properties = new HashMap<>();
        config.asMap().forEach((key, value) -> properties.put("vars.rabbitmq." + key, value));
        return properties;
    }
}
