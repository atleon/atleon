package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sqs.SqsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Map;

@Configuration
public class SqsConfiguration {

    @Bean("sqsInputQueueUrl")
    public String sqsInputQueueUrl(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Value("${stream.sqs.input.queue.name}") String queueName
    ) {
        return createQueueUrl(awsProperties, queueName);
    }

    private static String createQueueUrl(Map<String, ?> awsProperties, String queueName) {
        try (SqsAsyncClient client = SqsConfig.create(awsProperties).buildClient()) {
            return client.createQueue(builder -> builder.queueName(queueName)).join().queueUrl();
        }
    }
}
