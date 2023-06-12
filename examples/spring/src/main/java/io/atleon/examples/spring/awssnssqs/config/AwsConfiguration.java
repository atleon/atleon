package io.atleon.examples.spring.awssnssqs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class AwsConfiguration {

    @Bean("exampleAwsSnsSqsProperties")
    @ConfigurationProperties(prefix = "example.aws.sns.sqs")
    public Map<String, String> exampleAwsSnsSqsProperties() {
        return new HashMap<>();
    }
}
