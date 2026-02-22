package io.atleon.examples.endtoendtoend;

import io.atleon.aws.sqs.AtleonSqsAsyncClientSupplier;
import io.atleon.aws.sqs.ConfigurableSqsAsyncClientSupplier;
import io.atleon.aws.sqs.SqsReceiver;
import io.atleon.aws.sqs.SqsReceiverMessage;
import io.atleon.aws.sqs.SqsReceiverOptions;
import io.atleon.aws.sqs.SqsSender;
import io.atleon.aws.sqs.SqsSenderMessage;
import io.atleon.aws.sqs.SqsSenderOptions;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import io.atleon.aws.util.SdkConfig;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * This example shows how to work with Atleon's low-level SQS Receiver and Sender.
 */
public class SqsLowLevel {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    public static void main(String[] args) throws Exception {
        // Step 1) Create queue
        String queueUrl = createQueueAndGetUrl("my-queue");

        // Step 2) Specify sending options
        SqsSenderOptions senderOptions = SqsSenderOptions.newBuilder()
                .clientProperties(createClientProperties())
                .build();

        // Step 3) Create Sender, and send messages periodically
        SqsSender sender = SqsSender.create(senderOptions);
        Disposable production = Flux.interval(Duration.ofMillis(500))
                .map(number -> SqsSenderMessage.newBuilder()
                        .body("This is message #" + number)
                        .build())
                .transform(messages -> sender.send(messages, queueUrl))
                .doFinally(__ -> sender.close())
                .subscribe();

        // Step 4) Specify reception options
        SqsReceiverOptions receiverOptions = SqsReceiverOptions.newBuilder()
                .clientProperties(createClientProperties())
                .build();

        // Step 5) Create Receiver, then apply consumption
        Disposable processing = SqsReceiver.create(receiverOptions)
                .receiveManual(queueUrl)
                .doOnNext(message -> System.out.println("Received message: " + message.body()))
                .subscribe(SqsReceiverMessage::delete);

        // Step 6) Wait for user to terminate, then dispose of resources (stop stream processes)
        System.in.read();
        production.dispose();
        processing.dispose();
        System.exit(0);
    }

    private static String createQueueAndGetUrl(String name) {
        ConfigurableSqsAsyncClientSupplier clientSupplier =
                ConfigurableSqsAsyncClientSupplier.load(createClientProperties(), AtleonSqsAsyncClientSupplier::new);
        try (SqsAsyncClient client = clientSupplier.getClient()) {
            CreateQueueRequest request =
                    CreateQueueRequest.builder().queueName(name).build();
            return client.createQueue(request).join().queueUrl();
        }
    }

    private static Map<String, Object> createClientProperties() {
        Map<String, Object> clientProperties = new HashMap<>();
        clientProperties.put(SdkConfig.SQS_ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSqsEndpointOverride());
        clientProperties.put(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC);
        clientProperties.put(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey());
        clientProperties.put(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey());
        clientProperties.put(AwsConfig.REGION_CONFIG, CONTAINER.getRegion());
        return clientProperties;
    }
}
