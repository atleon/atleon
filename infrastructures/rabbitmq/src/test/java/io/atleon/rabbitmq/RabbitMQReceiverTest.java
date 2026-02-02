package io.atleon.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import io.atleon.util.IOFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RabbitMQReceiverTest {

    private static final EmbeddedAmqpConfig EMBEDDED_AMQP_CONFIG = EmbeddedAmqp.start();

    private final String exchange = RabbitMQReceiverTest.class.getSimpleName() + UUID.randomUUID();

    private final String queue = RabbitMQReceiverTest.class.getSimpleName() + UUID.randomUUID();

    private final String routingKey = "key";

    @BeforeEach
    public void setup() throws IOException {
        executeOnChannel(it -> {
            // Configure and declare exchange
            it.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true, false, false, Collections.emptyMap());

            // Configure and declare queue(s)
            it.queueDeclare(queue, true, false, false, Collections.emptyMap());

            // Bind queue(s) to exchange
            it.queueBind(queue, exchange, routingKey);

            return Optional.empty();
        });
    }

    @Test
    public void receiveManual_givenMessageSettledWithAck_expectsProcessingContinuation() throws IOException {
        String bodyText1 = "data1";
        String bodyText2 = "data2";

        RabbitMQSenderMessage<Object> message1 = RabbitMQSenderMessage.newBuilder()
                .exchange(exchange)
                .routingKey(routingKey)
                .body(bodyText1.getBytes(StandardCharsets.UTF_8))
                .build();
        RabbitMQSenderMessage<Object> message2 = RabbitMQSenderMessage.newBuilder()
                .exchange(exchange)
                .routingKey(routingKey)
                .body(bodyText2.getBytes(StandardCharsets.UTF_8))
                .build();

        sendMessages(message1, message2);

        RabbitMQReceiverOptions options = RabbitMQReceiverOptions.newBuilder()
                .connectionProperties(EMBEDDED_AMQP_CONFIG.asMap())
                .build();

        RabbitMQReceiver.create(options)
                .receiveManual(queue)
                .doOnNext(RabbitMQReceiverMessage::settleWithAck)
                .as(StepVerifier::create)
                .expectNextMatches(it -> Arrays.equals(bodyText1.getBytes(StandardCharsets.UTF_8), it.body()))
                .thenCancel()
                .verify();

        RabbitMQReceiver.create(options)
                .receiveManual(queue)
                .doOnNext(RabbitMQReceiverMessage::settleWithAck)
                .as(StepVerifier::create)
                .expectNextMatches(it -> Arrays.equals(bodyText2.getBytes(StandardCharsets.UTF_8), it.body()))
                .thenCancel()
                .verify();

        assertEquals(0, (long) executeOnChannel(it -> it.messageCount(queue)));
    }

    @Test
    public void receiveManual_givenMessageSettledWithRequeue_expectsProcessingRepetition() throws IOException {
        String bodyText1 = "data1";

        RabbitMQSenderMessage<Object> message1 = RabbitMQSenderMessage.newBuilder()
                .exchange(exchange)
                .routingKey(routingKey)
                .body(bodyText1.getBytes(StandardCharsets.UTF_8))
                .build();

        sendMessages(message1);

        RabbitMQReceiverOptions options = RabbitMQReceiverOptions.newBuilder()
                .connectionProperties(EMBEDDED_AMQP_CONFIG.asMap())
                .build();

        RabbitMQReceiver.create(options)
                .receiveManual(queue)
                .doOnNext(RabbitMQReceiverMessage::settleWithRequeue)
                .as(StepVerifier::create)
                .expectNextMatches(it -> Arrays.equals(bodyText1.getBytes(StandardCharsets.UTF_8), it.body()))
                .thenCancel()
                .verify();

        RabbitMQReceiver.create(options)
                .receiveManual(queue)
                .doOnNext(RabbitMQReceiverMessage::settleWithRequeue)
                .as(StepVerifier::create)
                .expectNextMatches(RabbitMQReceiverMessage::redeliver)
                .thenCancel()
                .verify();

        assertEquals(1, (long) executeOnChannel(it -> it.messageCount(queue)));
    }

    @Test
    public void receiveManual_givenMessageSettledWithReject_expectsRetainment() throws IOException {
        String bodyText1 = "data1";

        RabbitMQSenderMessage<Object> message1 = RabbitMQSenderMessage.newBuilder()
                .exchange(exchange)
                .routingKey(routingKey)
                .body(bodyText1.getBytes(StandardCharsets.UTF_8))
                .build();

        sendMessages(message1);

        RabbitMQReceiverOptions options = RabbitMQReceiverOptions.newBuilder()
                .connectionProperties(EMBEDDED_AMQP_CONFIG.asMap())
                .build();

        RabbitMQReceiver.create(options)
                .receiveManual(queue)
                .doOnNext(RabbitMQReceiverMessage::settleWithReject)
                .as(StepVerifier::create)
                .expectNextMatches(it -> Arrays.equals(bodyText1.getBytes(StandardCharsets.UTF_8), it.body()))
                .thenCancel()
                .verify();

        // AMQP handles rejection the same as requeue. Would be better to be able to configure DLQ.
        assertEquals(1, (long) executeOnChannel(it -> it.messageCount(queue)));
    }

    @SafeVarargs
    private static void sendMessages(RabbitMQSenderMessage<Object>... messages) {
        RabbitMQSenderOptions options = RabbitMQSenderOptions.newBuilder()
                .connectionProperties(EMBEDDED_AMQP_CONFIG.asMap())
                .build();
        RabbitMQSender sender = RabbitMQSender.create(options);
        try {
            Flux.just(messages).as(sender::send).then().block();
        } finally {
            sender.close();
        }
    }

    private static <T> T executeOnChannel(IOFunction<Channel, T> function) throws IOException {
        ConfiguratorConnectionSupplier connectionSupplier = new ConfiguratorConnectionSupplier();
        connectionSupplier.configure(EMBEDDED_AMQP_CONFIG.asMap());
        try (Connection connection = connectionSupplier.getConnection()) {
            return function.apply(connection.createChannel());
        }
    }
}
