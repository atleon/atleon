package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

import java.util.Map;

@Configuration
public class SnsConfiguration {

    @Bean("snsInputTopicArn")
    public String snsInputTopicArn(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Value("${stream.sns.input.topic.name}") String topicName
    ) {
        return createTopicArn(awsProperties, topicName);
    }

    private static String createTopicArn(Map<String, ?> awsProperties, String topicName) {
        try (SnsAsyncClient client = SnsConfig.create(awsProperties).buildClient()) {
            return client.createTopic(builder -> builder.name(topicName)).join().topicArn();
        }
    }
}
