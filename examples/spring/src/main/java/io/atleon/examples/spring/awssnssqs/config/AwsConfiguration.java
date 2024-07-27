package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sqs.SqsConfigSource;
import org.springframework.beans.factory.annotation.Qualifier;
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

    @Bean("exampleAwsSnsConfigSource")
    public SnsConfigSource exampleAwsSnsConfigSource(@Qualifier("exampleAwsSnsSqsProperties") Map<String, String> properties) {
        return SnsConfigSource.unnamed().withAll(properties);
    }

    @Bean("exampleAwsSqsConfigSource")
    public SqsConfigSource exampleAwsSqsConfigSource(@Qualifier("exampleAwsSnsSqsProperties") Map<String, String> properties) {
        return SqsConfigSource.unnamed().withAll(properties);
    }
}
