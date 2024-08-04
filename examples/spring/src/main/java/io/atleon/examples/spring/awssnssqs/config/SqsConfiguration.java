package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sqs.SqsConfigSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@Configuration
public class SqsConfiguration {

    @Bean("sqsInputQueueUrl")
    public String sqsInputQueueUrl(SqsConfigSource exampleSqsConfigSource, @Value("${stream.sqs.input.queue.name}") String queueName) {
        return createQueueUrl(exampleSqsConfigSource, queueName);
    }

    @Bean("sqsOutputQueueUrl")
    public String sqsOutputQueueUrl(SqsConfigSource exampleSqsConfigSource, @Value("${stream.sqs.output.queue.name}") String queueName) {
        return createQueueUrl(exampleSqsConfigSource, queueName);
    }

    private static String createQueueUrl(SqsConfigSource configSource, String queueName) {
        try (SqsAsyncClient client = configSource.create().block().buildClient()) {
            return client.createQueue(builder -> builder.queueName(queueName)).join().queueUrl();
        }
    }
}
