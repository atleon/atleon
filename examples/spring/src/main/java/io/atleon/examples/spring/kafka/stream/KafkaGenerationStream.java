package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

@AutoConfigureStream
@Profile("!integrationTest")
public class KafkaGenerationStream extends SelfConfigurableAloStream {

    private final KafkaConfigSource configSource;

    private final Environment environment;

    public KafkaGenerationStream(KafkaConfigSource exampleKafkaConfigSource, Environment environment) {
        this.configSource = exampleKafkaConfigSource;
        this.environment = environment;
    }

    @Override
    public Disposable startDisposable() {
        AloKafkaSender<Long, Long> sender = buildKafkaLongSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendValues(topic(), Function.identity()))
            .doFinally(sender::close)
            .subscribe();
    }

    private AloKafkaSender<Long, Long> buildKafkaLongSender() {
        return configSource.withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class)
            .as(AloKafkaSender::create);
    }

    private String topic() {
        return environment.getRequiredProperty("stream.kafka.input.topic");
    }
}
