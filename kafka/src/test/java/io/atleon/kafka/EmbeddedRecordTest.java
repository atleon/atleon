package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EmbeddedRecordTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    private static final KafkaConfigSource KAFKA_CONFIG_SOURCE = TestKafkaConfigFactory.createSource(BOOTSTRAP_CONNECT);

    private static final String TOPIC = EmbeddedRecordTest.class.getSimpleName();

    @Test
    public void consumedRecordsMatchSendValues() {
        String value = UUID.randomUUID().toString();

        AloKafkaSender.<String>forValues(KAFKA_CONFIG_SOURCE)
            .sendValues(Mono.just(value), TOPIC, Function.identity())
            .then().block();

        AloKafkaReceiver.<String>forValues(KAFKA_CONFIG_SOURCE)
            .receiveAloValues(Collections.singletonList(TOPIC))
            .as(StepVerifier::create)
            .consumeNextWith(aloString -> aloString.consume(string -> assertEquals(value, string), Alo::acknowledge))
            .thenCancel()
            .verify();
    }
}
