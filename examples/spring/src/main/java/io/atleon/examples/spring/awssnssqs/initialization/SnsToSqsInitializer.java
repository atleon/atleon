package io.atleon.examples.spring.awssnssqs.initialization;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
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
import java.util.UUID;

@Component
public class SnsToSqsInitializer implements ApplicationListener<ApplicationContextEvent>, Ordered {

    private final SnsAsyncClient snsClient;

    private final SqsAsyncClient sqsClient;

    private final String topicArn;

    private final String queueUrl;

    public SnsToSqsInitializer(
        SnsAsyncClient snsClient,
        SqsAsyncClient sqsClient,
        @Qualifier("snsTopicArn") String topicArn,
        @Qualifier("sqsQueueUrl") String queueUrl
    ) {
        this.snsClient = snsClient;
        this.sqsClient = sqsClient;
        this.topicArn = topicArn;
        this.queueUrl = queueUrl;
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            subscribeSqsQueueToSnsTopic();
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    private void subscribeSqsQueueToSnsTopic() {
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
    }
}
