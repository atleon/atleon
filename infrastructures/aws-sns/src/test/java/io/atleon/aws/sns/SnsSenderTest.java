package io.atleon.aws.sns;

import io.atleon.aws.sqs.SqsReceiver;
import io.atleon.aws.sqs.SqsReceiverMessage;
import io.atleon.aws.sqs.SqsReceiverOptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SnsSenderTest extends LocalStackDependentTest {

    private final SnsSender sender = SnsSender.create(newSenderOptions());

    @Test
    public void aMessageCanBeSentToSnsAndReceivedFromSubscribers() {
        String messageBody = UUID.randomUUID().toString();

        sender.send(toSenderMessage(messageBody), SnsAddress.topicArn(topicArn))
                .then()
                .block();

        Set<String> receivedBodies = SqsReceiver.create(newReceiverOptions())
                .receiveManual(queueUrl)
                .doOnNext(SqsReceiverMessage::delete)
                .map(SqsReceiverMessage::body)
                .take(1)
                .collect(Collectors.toSet())
                .block();

        assertEquals(Collections.singleton(messageBody), receivedBodies);
    }

    @Test
    public void manyMessagesCanBeSentToAndReceivedFromSubscribers() {
        Set<String> messageBodies = IntStream.range(0, 10)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toSet());

        Flux.fromIterable(messageBodies)
                .map(SnsSenderTest::toSenderMessage)
                .transform(messages -> sender.send(messages, topicArn))
                .then()
                .block();

        Set<String> receivedBodies = SqsReceiver.create(newReceiverOptions())
                .receiveManual(queueUrl)
                .doOnNext(SqsReceiverMessage::delete)
                .map(SqsReceiverMessage::body)
                .take(messageBodies.size())
                .collect(Collectors.toSet())
                .block();

        assertEquals(messageBodies, receivedBodies);
    }

    private SnsSenderOptions newSenderOptions() {
        return SnsSenderOptions.newBuilder()
                .clientProperties(createSnsClientProperties())
                .build();
    }

    private SqsReceiverOptions newReceiverOptions() {
        return SqsReceiverOptions.newBuilder()
                .clientProperties(createSqsClientProperties())
                .build();
    }

    private static SnsSenderMessage<Void> toSenderMessage(String body) {
        return SnsSenderMessage.<Void>newBuilder().body(body).build();
    }
}
