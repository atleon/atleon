package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Configuration
public class LocalStackConfiguration {

    private static final AtleonLocalStackContainer CONTAINER = startContainer();

    @Bean("awsRegion")
    public String awsRegion() {
        return CONTAINER.getRegion();
    }

    @Bean("awsAccessKeyId")
    public String awsAccessKeyId() {
        return CONTAINER.getAccessKey();
    }

    @Bean("awsSecretAccessKey")
    public String awsSecretAccessKey() {
        return CONTAINER.getSecretKey();
    }

    @Bean("snsEndpointOverride")
    public URI snsEndpointOverride() {
        return CONTAINER.getSnsEndpointOverride();
    }

    @Bean("sqsEndpointOverride")
    public URI sqsEndpointOverride() {
        return CONTAINER.getSqsEndpointOverride();
    }

    private static AtleonLocalStackContainer startContainer() {
        AtleonLocalStackContainer container = new AtleonLocalStackContainer();
        container.start();
        return container;
    }
}
