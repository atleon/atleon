package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.kafka.embedded.EmbeddedKafka;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class AloProcessingTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    private static final KafkaConfigSource KAFKA_CONFIG_SOURCE =
            TestKafkaConfigSourceFactory.createSource(BOOTSTRAP_CONNECT);

    private final String topic = AloProcessingTest.class.getSimpleName() + UUID.randomUUID();

    @Test
    public void acknowledgedDataIsNotRepublished() {
        AloKafkaSender.create(KAFKA_CONFIG_SOURCE)
                .sendValues(Mono.just("DATA"), topic, Function.identity())
                .then()
                .block();

        AloKafkaReceiver.create(KAFKA_CONFIG_SOURCE)
                .receiveAloValues(Collections.singletonList(topic))
                .as(StepVerifier::create)
                .consumeNextWith(Alo::acknowledge)
                .thenCancel()
                .verify();

        AloKafkaReceiver.create(KAFKA_CONFIG_SOURCE)
                .receiveAloValues(Collections.singletonList(topic))
                .as(StepVerifier::create)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(10L))
                .thenCancel()
                .verify();
    }

    @Test
    public void unacknowledgedDataIsRepublished() {
        AloKafkaSender.create(KAFKA_CONFIG_SOURCE)
                .sendValues(Mono.just("DATA"), topic, Function.identity())
                .then()
                .block();

        AloKafkaReceiver.create(KAFKA_CONFIG_SOURCE)
                .receiveAloValues(Collections.singletonList(topic))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .thenCancel()
                .verify();

        AloKafkaReceiver.create(KAFKA_CONFIG_SOURCE)
                .receiveAloValues(Collections.singletonList(topic))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .thenCancel()
                .verify();
    }

    @Test
    public void nacknowledgedDataIsRepublished() {
        AloKafkaSender.create(KAFKA_CONFIG_SOURCE)
                .sendValues(Flux.just("DATA1", "DATA2", "DATA3"), topic, Function.identity())
                .then()
                .block();

        AloKafkaReceiver.create(KAFKA_CONFIG_SOURCE)
                .receiveAloValues(Collections.singletonList(topic))
                .resubscribeOnError(AloProcessingTest.class.getSimpleName(), Duration.ofSeconds(0L))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .consumeNextWith(alo -> Alo.nacknowledge(alo, new RuntimeException()))
                .expectNextCount(2)
                .thenCancel()
                .verify();
    }
}
