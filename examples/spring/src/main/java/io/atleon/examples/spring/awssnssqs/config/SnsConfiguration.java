package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sns.SnsConfigSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

@Configuration
public class SnsConfiguration {

    @Bean("snsInputTopicArn")
    public String snsInputTopicArn(SnsConfigSource exampleSnsConfigSource, @Value("${stream.sns.input.topic.name}") String topicName) {
        return createTopicArn(exampleSnsConfigSource, topicName);
    }

    private static String createTopicArn(SnsConfigSource configSource, String topicName) {
        SnsConfig snsConfig = configSource.create().block();
        try (SnsAsyncClient client = snsConfig.buildClient()) {
            return client.createTopic(builder -> builder.name(topicName)).join().topicArn();
        }
    }
}
