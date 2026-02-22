package io.atleon.aws.sqs;

import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import io.atleon.aws.util.SdkConfig;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LocalStackDependentTest {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    protected final String queueUrl = createQueue(UUID.randomUUID().toString());

    protected static Map<String, Object> createSqsClientProperties() {
        Map<String, Object> clientProperties = new HashMap<>();
        clientProperties.put(SdkConfig.SQS_ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSqsEndpointOverride());
        clientProperties.put(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        clientProperties.put(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey());
        clientProperties.put(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey());
        clientProperties.put(AwsConfig.REGION_CONFIG, CONTAINER.getRegion());
        return clientProperties;
    }

    private static String createQueue(String queueName) {
        ConfigurableSqsAsyncClientSupplier clientSupplier =
                ConfigurableSqsAsyncClientSupplier.load(createSqsClientProperties(), AtleonSqsAsyncClientSupplier::new);
        try (SqsAsyncClient client = clientSupplier.getClient()) {
            CreateQueueRequest request =
                    CreateQueueRequest.builder().queueName(queueName).build();
            return client.createQueue(request).join().queueUrl();
        }
    }
}
