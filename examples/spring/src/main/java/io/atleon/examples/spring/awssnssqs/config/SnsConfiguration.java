package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfig;
import io.atleon.spring.ConfigContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

@Configuration
public class SnsConfiguration {

    @Bean("snsInputTopicArn")
    public String snsInputTopicArn(ConfigContext context, @Value("${stream.sns.input.topic.name}") String topicName) {
        return createTopicArn(context, topicName);
    }

    private static String createTopicArn(ConfigContext context, String topicName) {
        SnsConfig snsConfig = SnsConfig.create(context.getPropertiesPrefixedBy("example.aws.sns.sqs"));
        try (SnsAsyncClient client = snsConfig.buildClient()) {
            return client.createTopic(builder -> builder.name(topicName)).join().topicArn();
        }
    }
}
