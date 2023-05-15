package io.atleon.aws.sns;

import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import java.net.URI;
import java.util.Collections;
import java.util.UUID;

public class LocalStackDependentTest {

    private static final AtleonLocalStackContainer CONTAINER = startContainer();

    protected String topicArn;

    protected String queueUrl;

    @BeforeEach
    public void beforeEach() {
        SnsAsyncClient snsClient = createSnsClient();
        SqsAsyncClient sqsClient = createSqsClient();

        // Create SNS Topic
        CreateTopicRequest createTopicRequest = CreateTopicRequest.builder()
            .name(UUID.randomUUID().toString())
            .build();
        topicArn = snsClient.createTopic(createTopicRequest).join()
            .topicArn();

        // Create SQS Queue
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
            .queueName(UUID.randomUUID().toString())
            .build();
        queueUrl = sqsClient.createQueue(createQueueRequest).join()
            .queueUrl();

        // Get Attributes from created SQS Queue needed for subsequent Policy setting
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributeNames(QueueAttributeName.QUEUE_ARN)
            .build();
        String queueArn = sqsClient.getQueueAttributes(getQueueAttributesRequest).join()
            .attributes()
            .get(QueueAttributeName.QUEUE_ARN);

        // Build SQS policy for allowing SNS to publish from the created Topic to created Queue
        String policyJson = "{\n" +
            "   \"Version\": \"2012-10-17\",\n" +
            "   \"Id\": \"" + UUID.randomUUID() + "\",\n" +
            "   \"Statement\": [{\n" +
            "      \"Sid\":\"topic-subscription-" + topicArn + "\",\n" +
            "      \"Effect\": \"Allow\",\n" +
            "      \"Principal\": \"*\",\n" +
            "      \"Action\": \"sqs:SendMessage\",\n" +
            "      \"Resource\": \"" + queueArn + "\"\n" +
            "      \"Condition\": {\n" +
            "         \"ArnEquals\": {\n" +
            "            \"aws:SourceArn\": \"" + topicArn + "\"\n" +
            "         }\n" +
            "      }\n" +
            "   }]\n" +
            "}";

        // Set the built policy on the SQS Queue
        SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributes(Collections.singletonMap(QueueAttributeName.POLICY, policyJson))
            .build();
        sqsClient.setQueueAttributes(setQueueAttributesRequest).join();

        // Subscribe the queue to the topic; Enable 'raw' message delivery to avoid unnecessary JSON wrapping with metadata
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

    protected static SnsAsyncClient createSnsClient() {
        return SnsAsyncClient.builder()
            .endpointOverride(getSnsEndpointOverride())
            .credentialsProvider(StaticCredentialsProvider.create(createAwsCredentials()))
            .region(Region.of(CONTAINER.getRegion()))
            .build();
    }

    protected static SqsAsyncClient createSqsClient() {
        return SqsAsyncClient.builder()
            .endpointOverride(getSqsEndpointOverride())
            .credentialsProvider(StaticCredentialsProvider.create(createAwsCredentials()))
            .region(Region.of(CONTAINER.getRegion()))
            .build();
    }

    protected static URI getSnsEndpointOverride() {
        return CONTAINER.getSnsEndpointOverride();
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

    private static AtleonLocalStackContainer startContainer() {
        AtleonLocalStackContainer container = new AtleonLocalStackContainer();
        container.start();
        return container;
    }

    private static AwsCredentials createAwsCredentials() {
        return AwsBasicCredentials.create(CONTAINER.getAccessKey(), CONTAINER.getSecretKey());
    }
}
