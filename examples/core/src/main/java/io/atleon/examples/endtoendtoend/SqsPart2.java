package io.atleon.examples.endtoendtoend;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.ComposedSqsMessage;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodyDeserializer;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import io.atleon.aws.util.SdkConfig;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.io.IOException;
import java.time.Duration;

/**
 * This example shows how to work with Atleon's {@link AloSqsReceiver} and {@link AloSqsSender}
 * for processing messages from one queue to another.
 */
public class SqsPart2 {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    public static void main(String[] args) {
        // Step 1) Create input and output queues
        String inputQueueUrl = createQueueAndGetUrl("my-input-queue");
        String outputQueueUrl = createQueueAndGetUrl("my-output-queue");

        // Step 2) Periodically produce messages asynchronously
        Disposable production = periodicallyProduceMessages(inputQueueUrl, Duration.ofMillis(250));

        // Step 3) Specify reception and sending config
        SqsConfigSource configSource = createConfigSource()
                .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class)
                .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class);

        // Step 4) Create Sender and Receiver, then apply processing
        AloSqsSender<String> sender = AloSqsSender.create(configSource);
        Disposable processing = AloSqsReceiver.<String>create(configSource)
                .receiveAloMessages(inputQueueUrl)
                .map(message -> message.body().toUpperCase())
                .transform(sender.sendAloBodies(ComposedSqsMessage::fromBody, outputQueueUrl))
                .doOnNext(senderResult -> System.out.println("Sent message: " + senderResult.correlationMetadata()))
                .doFinally(sender::close)
                .subscribeWith(new DefaultAloSenderResultSubscriber<>());

        // Step 5) Wait for user to terminate, then dispose of resources (stop stream processes)
        awaitTerminationByUser();
        production.dispose();
        processing.dispose();
    }

    private static String createQueueAndGetUrl(String name) {
        try (SqsAsyncClient client = createConfigSource().create().block().buildClient()) {
            CreateQueueRequest request =
                    CreateQueueRequest.builder().queueName(name).build();
            return client.createQueue(request).join().queueUrl();
        }
    }

    private static Disposable periodicallyProduceMessages(String queueUrl, Duration period) {
        // Step 1) Specify sending config
        SqsConfigSource configSource =
                createConfigSource().with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class);

        // Step 2) Create Sender, and produce messages periodically
        AloSqsSender<String> sender = AloSqsSender.create(configSource);
        return Flux.interval(period)
                .map(number -> ComposedSqsMessage.fromBody("This is message #" + number))
                .transform(messages -> sender.sendMessages(messages, queueUrl))
                .doFinally(__ -> sender.close())
                .subscribe();
    }

    private static SqsConfigSource createConfigSource() {
        return SqsConfigSource.unnamed()
                .with(SdkConfig.SQS_ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSqsEndpointOverride())
                .with(AwsConfig.REGION_CONFIG, CONTAINER.getRegion())
                .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
                .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey())
                .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey());
    }

    private static void awaitTerminationByUser() {
        try {
            System.in.read();
        } catch (IOException e) {
            System.err.println("Failed to await termination by user: " + e);
        }
    }
}
