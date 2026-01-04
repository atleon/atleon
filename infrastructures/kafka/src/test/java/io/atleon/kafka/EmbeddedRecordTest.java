package io.atleon.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.atleon.core.Alo;
import io.atleon.kafka.embedded.EmbeddedKafka;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class EmbeddedRecordTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    private static final KafkaConfigSource KAFKA_CONFIG_SOURCE =
            TestKafkaConfigSourceFactory.createSource(BOOTSTRAP_CONNECT);

    private static final String TOPIC = EmbeddedRecordTest.class.getSimpleName();

    @Test
    public void consumedRecordsMatchSent() {
        String value = UUID.randomUUID().toString();

        AloKafkaSender.<String, String>create(KAFKA_CONFIG_SOURCE)
                .sendValues(Mono.just(value), TOPIC, Function.identity())
                .then()
                .block();

        AloKafkaReceiver.<Object, String>create(KAFKA_CONFIG_SOURCE)
                .receiveAloValues(Collections.singletonList(TOPIC))
                .as(StepVerifier::create)
                .consumeNextWith(aloString -> {
                    assertEquals(value, aloString.get());
                    Alo.acknowledge(aloString);
                })
                .thenCancel()
                .verify(Duration.ofSeconds(30));
    }
}
