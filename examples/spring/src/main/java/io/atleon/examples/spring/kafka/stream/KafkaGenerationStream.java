package io.atleon.examples.spring.kafka.stream;

import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

@AutoConfigureStream
@Profile("!integrationTest")
public class KafkaGenerationStream extends SpringAloStream {

    private final KafkaConfigSource configSource;

    public KafkaGenerationStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleKafkaConfigSource", KafkaConfigSource.class);
    }

    @Override
    public Disposable startDisposable() {
        AloKafkaSender<Long, Long> sender = buildKafkaLongSender();
        String topic = getRequiredProperty("stream.kafka.input.topic");

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendValues(topic, Function.identity()))
            .doFinally(sender::close)
            .subscribe();
    }

    private AloKafkaSender<Long, Long> buildKafkaLongSender() {
        return configSource.withClientId(name())
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class)
            .as(AloKafkaSender::create);
    }
}
