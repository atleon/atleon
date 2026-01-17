package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalStackInitializer implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Set<String> activeProfiles = Stream.of(environment.getActiveProfiles()).collect(Collectors.toSet());
        if (activeProfiles.contains("aws") && !activeProfiles.contains("integrationTest")) {
            AtleonLocalStackContainer container = AtleonLocalStackContainer.createAndStart();
            environment
                    .getPropertySources()
                    .addFirst(new MapPropertySource("local-stack", createProperties(container)));
        }
    }

    private static Map<String, Object> createProperties(AtleonLocalStackContainer container) {
        Map<String, Object> properties = new HashMap<>();

        properties.put("vars.aws.region", container.getRegion());
        properties.put("vars.aws.credentials.provider.type", AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        properties.put("vars.aws.credentials.access.key.id", container.getAccessKey());
        properties.put("vars.aws.credentials.secret.access.key", container.getSecretKey());
        properties.put(
                "vars.sns.endpoint.override", container.getSnsEndpointOverride().toString());
        properties.put(
                "vars.sqs.endpoint.override", container.getSqsEndpointOverride().toString());

        return properties;
    }
}
