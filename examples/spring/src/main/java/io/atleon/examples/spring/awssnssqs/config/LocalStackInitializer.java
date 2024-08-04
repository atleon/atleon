package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sqs.SqsConfig;
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
            environment.getPropertySources()
                .addLast(new MapPropertySource("local-stack", createProperties(container)));
        }
    }

    private static Map<String, Object> createProperties(AtleonLocalStackContainer container) {
        Map<String, Object> properties = new HashMap<>();

        properties.put("atleon.config.sources[0].name", "exampleSnsConfigSource");
        properties.put("atleon.config.sources[0].type", "sns");
        properties.put("atleon.config.sources[0]." + AwsConfig.REGION_CONFIG, container.getRegion());
        properties.put("atleon.config.sources[0]." + AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        properties.put("atleon.config.sources[0]." + AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, container.getAccessKey());
        properties.put("atleon.config.sources[0]." + AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, container.getSecretKey());
        properties.put("atleon.config.sources[0]." + SnsConfig.ENDPOINT_OVERRIDE_CONFIG, container.getSnsEndpointOverride().toString());

        properties.put("atleon.config.sources[1].name", "exampleSqsConfigSource");
        properties.put("atleon.config.sources[1].type", "sqs");
        properties.put("atleon.config.sources[1]." + AwsConfig.REGION_CONFIG, container.getRegion());
        properties.put("atleon.config.sources[1]." + AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        properties.put("atleon.config.sources[1]." + AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, container.getAccessKey());
        properties.put("atleon.config.sources[1]." + AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, container.getSecretKey());
        properties.put("atleon.config.sources[1]." + SqsConfig.ENDPOINT_OVERRIDE_CONFIG, container.getSqsEndpointOverride().toString());

        return properties;
    }
}
