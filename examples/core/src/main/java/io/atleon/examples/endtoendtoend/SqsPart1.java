package io.atleon.examples.endtoendtoend;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.ComposedSqsMessage;
import io.atleon.aws.sqs.SqsConfig;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodyDeserializer;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import io.atleon.core.Alo;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.time.Duration;

/**
 * This example shows how to work with Atleon's {@link AloSqsReceiver} for consuming messages.
 */
public class SqsPart1 {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    public static void main(String[] args) {
        //Step 1) Create queue, then determine number of messages
        String queueUrl = createQueueAndGetUrl("my-queue");
        int numberOfMessages = args.length < 1 ? 20 : Integer.parseInt(args[0]);

        //Step 2) Periodically produce number of messages asynchronously
        periodicallyProduceMessages(queueUrl, Duration.ofMillis(250), numberOfMessages);

        //Step 3) Specify reception config
        SqsConfigSource configSource = createConfigSource()
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class);

        //Step 4) Create Receiver, then apply consumption
        AloSqsReceiver.from(configSource)
            .receiveAloMessages(queueUrl)
            .doOnNext(message -> System.out.println("Received message: " + message.body()))
            .consumeAloAndGet(Alo::acknowledge)
            .take(numberOfMessages)
            .blockLast();
    }

    private static String createQueueAndGetUrl(String name) {
        try (SqsAsyncClient client = createConfigSource().create().block().buildClient()) {
            CreateQueueRequest request = CreateQueueRequest.builder().queueName(name).build();
            return client.createQueue(request).join().queueUrl();
        }
    }

    private static void periodicallyProduceMessages(String queueUrl, Duration period, int numberOfMessages) {
        //Step 1) Specify sending config
        SqsConfigSource configSource = createConfigSource()
            .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class);

        //Step 2) Create Sender, and produce number of messages periodically
        AloSqsSender<String> sender = AloSqsSender.from(configSource);
        Flux.range(1, numberOfMessages)
            .delayElements(period)
            .map(number -> ComposedSqsMessage.fromBody("This is message #" + number))
            .transform(messages -> sender.sendMessages(messages, queueUrl))
            .doFinally(__ -> sender.close())
            .subscribe();
    }

    private static SqsConfigSource createConfigSource() {
        return SqsConfigSource.unnamed()
            .with(AwsConfig.REGION_CONFIG, CONTAINER.getRegion())
            .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
            .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey())
            .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey())
            .with(SqsConfig.ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSqsEndpointOverride());
    }
}
