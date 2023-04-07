package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class LocalStackConfiguration {

    private static final AtleonLocalStackContainer CONTAINER = startContainer();

    @Bean("localAws")
    public Map<String, Object> localAws() {
        Map<String, Object> localAwsProperties = new HashMap<>();
        localAwsProperties.put(AwsConfig.REGION_CONFIG, CONTAINER.getRegion());
        localAwsProperties.put(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        localAwsProperties.put(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey());
        localAwsProperties.put(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey());
        return localAwsProperties;
    }

    @Bean("localSnsEndpoint")
    public URI snsEndpoint() {
        return CONTAINER.getSnsEndpointOverride();
    }

    @Bean("localSqsEndpoint")
    public URI sqsEndpoint() {
        return CONTAINER.getSqsEndpointOverride();
    }

    private static AtleonLocalStackContainer startContainer() {
        AtleonLocalStackContainer container = new AtleonLocalStackContainer();
        container.start();
        return container;
    }
}
