package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import io.atleon.core.Alo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EmbeddedMessageTest {

    private static final EmbeddedAmqpConfig EMBEDDED_AMQP_CONFIG = EmbeddedAmqp.start();

    private static final RabbitMQConfigSource RABBIT_MQ_CONFIG_SOURCE = TestRabbitMQSourceFactory.createStringSource(EMBEDDED_AMQP_CONFIG);

    private final String queue = EmbeddedMessageTest.class.getSimpleName();

    @BeforeEach
    public void setup() throws Exception {
        ConnectionFactory connectionFactory = RABBIT_MQ_CONFIG_SOURCE.createConnectionFactoryNow();
        try (Connection connection = connectionFactory.newConnection()) {
            connection.createChannel()
                .queueDeclare(queue, false, false, false, null);
        }
    }

    @Test
    public void consumedMessagesMatchSent() {
        String body = UUID.randomUUID().toString();

        AloRabbitMQSender.<String>create(RABBIT_MQ_CONFIG_SOURCE)
            .sendBodies(Mono.just(body), DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(queue))
            .then().block();

        AloRabbitMQReceiver.<String>create(RABBIT_MQ_CONFIG_SOURCE)
            .receiveAloBodies(queue)
            .as(StepVerifier::create)
            .consumeNextWith(aloString -> {
                assertEquals(body, aloString.get());
                Alo.acknowledge(aloString);
            })
            .thenCancel()
            .verify();
    }
}
