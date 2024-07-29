package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sqs.SqsConfig;
import io.atleon.spring.ConfigContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@Configuration
public class SqsConfiguration {

    @Bean("sqsInputQueueUrl")
    public String sqsInputQueueUrl(ConfigContext context, @Value("${stream.sqs.input.queue.name}") String queueName) {
        return createQueueUrl(context, queueName);
    }

    @Bean("sqsOutputQueueUrl")
    public String sqsOutputQueueUrl(ConfigContext context, @Value("${stream.sqs.output.queue.name}") String queueName) {
        return createQueueUrl(context, queueName);
    }

    private static String createQueueUrl(ConfigContext context, String queueName) {
        SqsConfig sqsConfig = SqsConfig.create(context.getPropertiesPrefixedBy("example.aws.sns.sqs"));
        try (SqsAsyncClient client = sqsConfig.buildClient()) {
            return client.createQueue(builder -> builder.queueName(queueName)).join().queueUrl();
        }
    }
}
