package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.core.Alo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

public class AloProcessingTest {

    private static final Map<String, ?> AMQP_CONFIG = EmbeddedAmqp.start();

    private static final RabbitMQConfigSource RABBIT_MQ_CONFIG_SOURCE = TestRabbitMQSourceFactory.createStringSource(AMQP_CONFIG);

    private final String queue = AloProcessingTest.class.getSimpleName() + UUID.randomUUID();

    @BeforeEach
    public void setup() throws Exception {
        ConnectionFactory connectionFactory = RABBIT_MQ_CONFIG_SOURCE.create()
            .map(RabbitMQConfig::getConnectionFactory)
            .block();
        try (Connection connection = connectionFactory.newConnection()) {
            connection.createChannel()
                .queueDeclare(queue, false, false, false, null);
        }
    }

    @Test
    public void acknowledgedDataIsNotRepublished() {
        AloRabbitMQSender.from(RABBIT_MQ_CONFIG_SOURCE)
            .sendBodies(Mono.just("DATA"), DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(queue))
            .then().block();

        AloRabbitMQReceiver.from(RABBIT_MQ_CONFIG_SOURCE)
            .receiveAloBodies(queue)
            .as(StepVerifier::create)
            .consumeNextWith(Alo::acknowledge)
            .thenCancel()
            .verify();

        AloRabbitMQReceiver.from(RABBIT_MQ_CONFIG_SOURCE)
            .receiveAloBodies(queue)
            .as(StepVerifier::create)
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10L))
            .thenCancel()
            .verify();
    }

    @Test
    public void unacknowledgedDataIsRepublished() {
        AloRabbitMQSender.from(RABBIT_MQ_CONFIG_SOURCE)
            .sendBodies(Mono.just("DATA"), DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(queue))
            .then().block();

        AloRabbitMQReceiver.from(RABBIT_MQ_CONFIG_SOURCE)
            .receiveAloBodies(queue)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        AloRabbitMQReceiver.from(RABBIT_MQ_CONFIG_SOURCE)
            .receiveAloBodies(queue)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();
    }

    @Test
    public void nacknowledgedDataIsRepublished() {
        AloRabbitMQSender.from(RABBIT_MQ_CONFIG_SOURCE)
            .sendBodies(Flux.just("DATA1", "DATA2", "DATA3"), DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(queue))
            .then().block();

        AloRabbitMQReceiver.from(RABBIT_MQ_CONFIG_SOURCE)
            .receiveAloBodies(queue)
            .resubscribeOnError(AloProcessingTest.class.getSimpleName(), Duration.ofSeconds(0L))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .consumeNextWith(alo -> Alo.nacknowledge(alo, new RuntimeException()))
            .expectNextCount(2)
            .thenCancel()
            .verify();
    }
}
