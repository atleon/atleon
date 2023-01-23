package io.atleon.examples.spring.awssnssqs.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.net.URI;

@Component
public class SqsConfiguration {

    @Bean
    public SqsAsyncClient sqsClient(
        @Qualifier("sqsEndpointOverride") URI endpointOverride,
        @Qualifier("awsRegion") String region,
        @Qualifier("awsAccessKeyId") String accessKeyId,
        @Qualifier("awsSecretAccessKey") String secretAccessKey
    ) {
        return SqsAsyncClient.builder()
            .endpointOverride(endpointOverride)
            .region(Region.of(region))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
            .build();
    }

    @Bean("sqsQueueUrl")
    public String sqsQueueUrl(SqsAsyncClient client, @Value("${example.sqs.queue.name}") String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder().queueName(queueName).build();
        return client.createQueue(request).join().queueUrl();
    }
}
