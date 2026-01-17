package io.atleon.aws.sqs;

import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.net.URI;
import java.util.UUID;

public class LocalStackDependentTest {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    protected final String queueUrl = createQueue(UUID.randomUUID().toString());

    protected static SqsAsyncClient createSqsClient() {
        return SqsAsyncClient.builder()
                .endpointOverride(getSqsEndpointOverride())
                .credentialsProvider(StaticCredentialsProvider.create(createAwsCredentials()))
                .region(Region.of(CONTAINER.getRegion()))
                .build();
    }

    protected static URI getSqsEndpointOverride() {
        return CONTAINER.getSqsEndpointOverride();
    }

    protected static String getAccessKey() {
        return CONTAINER.getAccessKey();
    }

    protected static String getSecretKey() {
        return CONTAINER.getSecretKey();
    }

    protected static Region getRegion() {
        return Region.of(CONTAINER.getRegion());
    }

    private static AwsCredentials createAwsCredentials() {
        return AwsBasicCredentials.create(CONTAINER.getAccessKey(), CONTAINER.getSecretKey());
    }

    private static String createQueue(String queueName) {
        try (SqsAsyncClient client = createSqsClient()) {
            CreateQueueRequest request =
                    CreateQueueRequest.builder().queueName(queueName).build();
            return client.createQueue(request).join().queueUrl();
        }
    }
}
