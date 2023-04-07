package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.util.AwsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.net.URI;
import java.util.Map;

@Component
public class SqsConfiguration {

    @Bean
    public SqsAsyncClient sqsClient(
        @Qualifier("localSqsEndpoint") URI endpointOverride,
        @Value("#{localAws}") Map<String, ?> awsProperties
    ) {
        return SqsAsyncClient.builder()
            .endpointOverride(endpointOverride)
            .region(AwsConfig.loadRegion(awsProperties).orElse(null))
            .credentialsProvider(AwsConfig.loadCredentialsProvider(awsProperties))
            .build();
    }

    @Bean("sqsQueueUrl")
    public String sqsQueueUrl(SqsAsyncClient client, @Value("${example.sqs.queue.name}") String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder().queueName(queueName).build();
        return client.createQueue(request).join().queueUrl();
    }
}
