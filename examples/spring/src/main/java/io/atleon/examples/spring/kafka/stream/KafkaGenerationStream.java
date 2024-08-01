package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.ConfigContext;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

@AutoConfigureStream
@Profile("!integrationTest")
public class KafkaGenerationStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public KafkaGenerationStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    public Disposable startDisposable() {
        AloKafkaSender<Long, Long> sender = buildKafkaLongSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendValues(getTopic(), Function.identity()))
            .doFinally(sender::close)
            .subscribe();
    }

    private AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        return AloKafkaSender.create(configSource);
    }

    private String getTopic() {
        return context.getProperty("stream.kafka.input.topic");
    }
}
