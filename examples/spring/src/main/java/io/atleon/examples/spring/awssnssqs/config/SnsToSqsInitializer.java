package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sqs.SqsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@Component
public class SnsToSqsInitializer implements ApplicationListener<ContextRefreshedEvent>, Ordered {

    private final Map<String, ?> awsProperties;

    private final String topicArn;

    private final String queueUrl;

    public SnsToSqsInitializer(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Qualifier("snsInputTopicArn") String topicArn,
        @Qualifier("sqsInputQueueUrl") String queueUrl
    ) {
        this.awsProperties = awsProperties;
        this.topicArn = topicArn;
        this.queueUrl = queueUrl;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        String queueArn = initializeSqsQueueArn();
        try (SnsAsyncClient snsAsyncClient = SnsConfig.create(awsProperties).buildClient()) {
            subscribeSqsQueueToSnsTopic(snsAsyncClient, queueArn);
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    private String initializeSqsQueueArn() {
        try (SqsAsyncClient sqsClient = SqsConfig.create(awsProperties).buildClient()) {
            return initializeSqsQueueArn(sqsClient);
        }
    }

    private String initializeSqsQueueArn(SqsAsyncClient sqsClient) {
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

        return queueArn;
    }

    private void subscribeSqsQueueToSnsTopic(SnsAsyncClient snsClient, String queueArn) {
        // Subscribe the queue to the topic; Enable 'raw' message delivery to avoid unnecessary JSON wrapping with metadata
        SubscribeRequest subscribeRequest = SubscribeRequest.builder()
            .topicArn(topicArn)
            .protocol("sqs")
            .endpoint(queueArn)
            .attributes(Collections.singletonMap("RawMessageDelivery", "true"))
            .build();
        snsClient.subscribe(subscribeRequest).join();
    }
}
