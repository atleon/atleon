package io.atleon.aws.sns;

import io.atleon.aws.sqs.AtleonSqsAsyncClientSupplier;
import io.atleon.aws.sqs.ConfigurableSqsAsyncClientSupplier;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import io.atleon.aws.util.SdkConfig;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LocalStackDependentTest {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    protected String topicArn;

    protected String queueUrl;

    @BeforeEach
    public void beforeEach() {
        SnsAsyncClient snsClient = createSnsClient();
        SqsAsyncClient sqsClient = createSqsClient();

        // Create SNS Topic
        CreateTopicRequest createTopicRequest =
                CreateTopicRequest.builder().name(UUID.randomUUID().toString()).build();
        topicArn = snsClient.createTopic(createTopicRequest).join().topicArn();

        // Create SQS Queue
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString())
                .build();
        queueUrl = sqsClient.createQueue(createQueueRequest).join().queueUrl();

        // Get Attributes from created SQS Queue needed for subsequent Policy setting
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build();
        String queueArn = sqsClient
                .getQueueAttributes(getQueueAttributesRequest)
                .join()
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);

        // Build SQS policy for allowing SNS to publish from the created Topic to created Queue
        String policyJson = "{\n" + "   \"Version\": \"2012-10-17\",\n"
                + "   \"Id\": \""
                + UUID.randomUUID() + "\",\n" + "   \"Statement\": [{\n"
                + "      \"Sid\":\"topic-subscription-"
                + topicArn + "\",\n" + "      \"Effect\": \"Allow\",\n"
                + "      \"Principal\": \"*\",\n"
                + "      \"Action\": \"sqs:SendMessage\",\n"
                + "      \"Resource\": \""
                + queueArn + "\"\n" + "      \"Condition\": {\n"
                + "         \"ArnEquals\": {\n"
                + "            \"aws:SourceArn\": \""
                + topicArn + "\"\n" + "         }\n"
                + "      }\n"
                + "   }]\n"
                + "}";

        // Set the built policy on the SQS Queue
        SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributes(Collections.singletonMap(QueueAttributeName.POLICY, policyJson))
                .build();
        sqsClient.setQueueAttributes(setQueueAttributesRequest).join();

        // Subscribe the queue to the topic; Enable 'raw' message delivery to avoid unnecessary JSON wrapping with
        // metadata
        SubscribeRequest subscribeRequest = SubscribeRequest.builder()
                .topicArn(topicArn)
                .protocol("sqs")
                .endpoint(queueArn)
                .attributes(Collections.singletonMap("RawMessageDelivery", "true"))
                .build();
        snsClient.subscribe(subscribeRequest).join();

        snsClient.close();
        sqsClient.close();
    }

    protected static Map<String, Object> createSnsClientProperties() {
        Map<String, Object> clientProperties = createClientProperties();
        clientProperties.put(SdkConfig.SNS_ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSnsEndpointOverride());
        return clientProperties;
    }

    protected static Map<String, Object> createSqsClientProperties() {
        Map<String, Object> clientProperties = createClientProperties();
        clientProperties.put(SdkConfig.SQS_ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSqsEndpointOverride());
        return clientProperties;
    }

    private static SnsAsyncClient createSnsClient() {
        return ConfigurableSnsAsyncClientSupplier.load(createSnsClientProperties(), AtleonSnsAsyncClientSupplier::new)
                .getClient();
    }

    private static SqsAsyncClient createSqsClient() {
        return ConfigurableSqsAsyncClientSupplier.load(createSqsClientProperties(), AtleonSqsAsyncClientSupplier::new)
                .getClient();
    }

    private static Map<String, Object> createClientProperties() {
        Map<String, Object> clientProperties = new HashMap<>();
        clientProperties.put(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        clientProperties.put(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey());
        clientProperties.put(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey());
        clientProperties.put(AwsConfig.REGION_CONFIG, CONTAINER.getRegion());
        return clientProperties;
    }
}
