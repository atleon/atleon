package io.atleon.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import io.atleon.util.IOFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.AdditionalAnswers;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RabbitMQSenderTest {

    private static final EmbeddedAmqpConfig EMBEDDED_AMQP_CONFIG = EmbeddedAmqp.start();

    private final String exchange = RabbitMQSenderTest.class.getSimpleName() + UUID.randomUUID();

    private final String queue = RabbitMQSenderTest.class.getSimpleName() + UUID.randomUUID();

    private final String routingKey = RabbitMQSenderTest.class.getSimpleName() + UUID.randomUUID();

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
    public void send_givenSuccessfulSend_expectsProducedMessages() throws IOException {
        String body = UUID.randomUUID().toString();

        RabbitMQSenderMessage<Object> senderMessage = RabbitMQSenderMessage.newBuilder()
                .exchange(exchange)
                .routingKey(routingKey)
                .body(body.getBytes(StandardCharsets.UTF_8))
                .build();

        RabbitMQSenderOptions senderOptions = RabbitMQSenderOptions.newBuilder()
                .connectionProperties(EMBEDDED_AMQP_CONFIG.asMap())
                .build();

        executeOnSender(senderOptions, it -> it.send(senderMessage).block());

        GetResponse result = getMessage(queue);

        assertEquals(body, new String(result.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void send_givenFailedSendToNonExistentExchange_expectsError() {
        String body = UUID.randomUUID().toString();

        RabbitMQSenderMessage<Object> senderMessage = RabbitMQSenderMessage.newBuilder()
                .exchange(UUID.randomUUID().toString())
                .routingKey(routingKey)
                .body(body.getBytes(StandardCharsets.UTF_8))
                .build();

        RabbitMQSenderOptions senderOptions = RabbitMQSenderOptions.newBuilder()
                .connectionProperties(EMBEDDED_AMQP_CONFIG.asMap())
                .build();

        Executable send = () ->
                executeOnSender(senderOptions, it -> it.send(senderMessage).block());

        assertThrows(ShutdownSignalException.class, send);
    }

    @Test
    public void sendDelegateError_givenFailedSend_expectsSendResultsToReflectError() {
        String body = UUID.randomUUID().toString();

        RabbitMQSenderMessage<Object> senderMessage = RabbitMQSenderMessage.newBuilder()
                .exchange(exchange)
                .routingKey(routingKey)
                .body(body.getBytes(StandardCharsets.UTF_8))
                .build();

        RabbitMQSenderOptions senderOptions = RabbitMQSenderOptions.newBuilder(RabbitMQSenderTest::newBoomingConnection)
                .build();

        RabbitMQSenderResult<Object> result = executeOnSender(
                senderOptions,
                it -> Mono.just(senderMessage).as(it::sendDelegateError).blockFirst());

        assertNotNull(result);
        assertTrue(result.isFailure());
    }

    private static <T> T executeOnSender(RabbitMQSenderOptions senderOptions, Function<RabbitMQSender, T> function) {
        RabbitMQSender sender = RabbitMQSender.create(senderOptions);
        try {
            return function.apply(sender);
        } finally {
            sender.close();
        }
    }

    private static GetResponse getMessage(String queue) throws IOException {
        return executeOnChannel(channel -> channel.basicGet(queue, false));
    }

    private static <T> T executeOnChannel(IOFunction<Channel, T> function) throws IOException {
        try (Connection connection = newConnection()) {
            return function.apply(connection.createChannel());
        }
    }

    private static Connection newBoomingConnection() throws IOException {
        Connection connection = newConnection();
        Connection boomingConnection = mock(Connection.class, AdditionalAnswers.delegatesTo(connection));
        when(boomingConnection.createChannel()).thenAnswer(__ -> newBoomingChannel(connection));
        return boomingConnection;
    }

    private static Channel newBoomingChannel(Connection connection) throws IOException {
        Channel boomingChannel = mock(Channel.class, AdditionalAnswers.delegatesTo(connection.createChannel()));
        doThrow(new IOException("Boom"))
                .when(boomingChannel)
                .basicPublish(anyString(), anyString(), anyBoolean(), anyBoolean(), any(), any());
        return boomingChannel;
    }

    private static Connection newConnection() throws IOException {
        return ConfigurableConnectionSupplier.load(EMBEDDED_AMQP_CONFIG.asMap(), ConfiguratorConnectionSupplier::new)
                .getConnection();
    }
}
