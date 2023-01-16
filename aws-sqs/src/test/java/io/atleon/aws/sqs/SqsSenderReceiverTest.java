package io.atleon.aws.sqs;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqsSenderReceiverTest extends LocalStackDependentTest {

    private final SqsSender sender = SqsSender.create(newSenderOptions());

    @Test
    public void aMessageCanBeSentAndReceived() {
        String messageBody = UUID.randomUUID().toString();

        sender.send(toSenderMessage(messageBody), queueUrl).then().block();

        Set<String> receivedBodies = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .doOnNext(SqsReceiverMessage::delete)
            .map(SqsReceiverMessage::body)
            .take(1)
            .collect(Collectors.toSet())
            .block();

        assertEquals(Collections.singleton(messageBody), receivedBodies);
    }

    @Test
    public void manyMessagesCanBeSentAndReceived() {
        Set<String> messageBodies = IntStream.range(0, 10)
            .mapToObj(i -> UUID.randomUUID().toString())
            .collect(Collectors.toSet());

        Flux.fromIterable(messageBodies)
            .map(SqsSenderReceiverTest::toSenderMessage)
            .transform(messages -> sender.send(messages, queueUrl))
            .then().block();

        Set<String> receivedBodies = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .doOnNext(SqsReceiverMessage::delete)
            .map(SqsReceiverMessage::body)
            .take(messageBodies.size())
            .collect(Collectors.toSet())
            .block();

        assertEquals(messageBodies, receivedBodies);
    }

    @Test
    public void nonDeletedMessagesAreReceivedAgain() {
        String messageBody = UUID.randomUUID().toString();

        sender.send(toSenderMessage(messageBody), queueUrl).then().block();

        List<String> receivedBodies1 = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .map(SqsReceiverMessage::body)
            .take(1)
            .collectList()
            .block();

        List<String> receivedBodies2 = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .doOnNext(SqsReceiverMessage::delete)
            .map(SqsReceiverMessage::body)
            .take(1)
            .collectList()
            .block();

        assertEquals(1, receivedBodies1.size());
        assertEquals(messageBody, receivedBodies1.get(0));
        assertEquals(1, receivedBodies2.size());
        assertEquals(messageBody, receivedBodies2.get(0));
    }

    @Test
    public void deletedMessagesAreNotReceivedAgain() {
        String messageBody1 = UUID.randomUUID().toString();

        sender.send(toSenderMessage(messageBody1), queueUrl).block();

        List<String> messages1 = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .doOnNext(SqsReceiverMessage::delete)
            .map(SqsReceiverMessage::body)
            .take(1)
            .collectList()
            .block();

        String messageBody2 = UUID.randomUUID().toString();

        sender.send(toSenderMessage(messageBody2), queueUrl).block();

        List<String> messages2 = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .doOnNext(SqsReceiverMessage::delete)
            .map(SqsReceiverMessage::body)
            .take(1)
            .collectList()
            .block();

        assertEquals(1, messages1.size());
        assertEquals(messageBody1, messages1.get(0));
        assertEquals(1, messages2.size());
        assertEquals(messageBody2, messages2.get(0));
    }

    @Test
    public void messagesThatHaveVisibilityResetAreReceivedAgain() {
        String messageBody = UUID.randomUUID().toString();

        sender.send(toSenderMessage(messageBody), queueUrl).block();

        List<String> messages = SqsReceiver.create(newReceiverOptions()).receiveManual(queueUrl)
            .index((index, receivedMessage) -> {
                if (index == 0) {
                    receivedMessage.changeVisibility(Duration.ZERO);
                }
                return receivedMessage;
            })
            .map(SqsReceiverMessage::body)
            .take(2)
            .collectList()
            .block();

        assertEquals(2, messages.size());
        assertEquals(messageBody, messages.get(0));
        assertEquals(messageBody, messages.get(1));
    }

    private SqsSenderOptions newSenderOptions() {
        return SqsSenderOptions.defaultOptions(SqsSenderReceiverTest::createSqsClient);
    }

    private SqsReceiverOptions newReceiverOptions() {
        return SqsReceiverOptions.defaultOptions(SqsSenderReceiverTest::createSqsClient);
    }

    private static SqsSenderMessage<Void> toSenderMessage(String body) {
        return SqsSenderMessage.<Void>newBuilder().body(body).build();
    }
}