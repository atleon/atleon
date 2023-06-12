package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sqs.SqsConfig;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

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
            PropertySource<?> propertySource = new MapPropertySource("local.stack", createProperties(container));
            environment.getPropertySources().addLast(propertySource);
        }
    }

    private static Map<String, Object> createProperties(AtleonLocalStackContainer container) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("example.aws.sns.sqs." + AwsConfig.REGION_CONFIG, container.getRegion());
        properties.put("example.aws.sns.sqs." + AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        properties.put("example.aws.sns.sqs." + AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, container.getAccessKey());
        properties.put("example.aws.sns.sqs." + AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, container.getSecretKey());
        properties.put("example.aws.sns.sqs." + SnsConfig.ENDPOINT_OVERRIDE_CONFIG, container.getSnsEndpointOverride().toString());
        properties.put("example.aws.sns.sqs." + SqsConfig.ENDPOINT_OVERRIDE_CONFIG, container.getSqsEndpointOverride().toString());
        return properties;
    }
}
