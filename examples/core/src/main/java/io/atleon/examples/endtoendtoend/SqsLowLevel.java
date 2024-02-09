package io.atleon.examples.endtoendtoend;

import io.atleon.aws.sqs.SqsReceiver;
import io.atleon.aws.sqs.SqsReceiverMessage;
import io.atleon.aws.sqs.SqsReceiverOptions;
import io.atleon.aws.sqs.SqsSender;
import io.atleon.aws.sqs.SqsSenderMessage;
import io.atleon.aws.sqs.SqsSenderOptions;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.io.IOException;
import java.time.Duration;

/**
 * This example shows how to work with Atleon's low-level SQS Receiver and Sender.
 */
public class SqsLowLevel {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    public static void main(String[] args) {
        //Step 1) Create queue
        String queueUrl = createQueueAndGetUrl("my-queue");

        //Step 2) Periodically produce messages asynchronously
        Disposable production = periodicallyProduceMessages(queueUrl, Duration.ofMillis(500));

        //Step 3) Specify reception options
        SqsReceiverOptions options = SqsReceiverOptions.defaultOptions(SqsLowLevel::createClient);

        //Step 4) Create Receiver, then apply consumption
        Disposable processing = SqsReceiver.create(options)
            .receiveManual(queueUrl)
            .doOnNext(message -> System.out.println("Received message: " + message.body()))
            .subscribe(SqsReceiverMessage::delete);

        //Step 5) Wait for user to terminate, then dispose of resources (stop stream processes)
        awaitTerminationByUser();
        production.dispose();
        processing.dispose();
    }

    private static String createQueueAndGetUrl(String name) {
        try (SqsAsyncClient client = createClient()) {
            CreateQueueRequest request = CreateQueueRequest.builder().queueName(name).build();
            return client.createQueue(request).join().queueUrl();
        }
    }

    private static Disposable periodicallyProduceMessages(String queueUrl, Duration period) {
        //Step 1) Specify sending options
        SqsSenderOptions options = SqsSenderOptions.defaultOptions(SqsLowLevel::createClient);

        //Step 2) Create Sender, and messages periodically
        SqsSender sender = SqsSender.create(options);
        return Flux.interval(period)
            .map(number -> SqsSenderMessage.newBuilder().body("This is message #" + (number + 1)).build())
            .transform(messages -> sender.send(messages, queueUrl))
            .doFinally(__ -> sender.close())
            .subscribe();
    }

    private static SqsAsyncClient createClient() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(CONTAINER.getAccessKey(), CONTAINER.getSecretKey());
        return SqsAsyncClient.builder()
            .region(Region.of(CONTAINER.getRegion()))
            .endpointOverride(CONTAINER.getSqsEndpointOverride())
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();
    }

    private static void awaitTerminationByUser() {
        try {
            System.in.read();
        } catch (IOException e) {
            System.err.println("Failed to await termination by user: " + e);
        }
    }
}
