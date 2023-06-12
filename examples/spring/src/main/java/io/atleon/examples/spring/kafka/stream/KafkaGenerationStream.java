package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStream;
import io.atleon.kafka.AloKafkaSender;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

public class KafkaGenerationStream extends AloStream<KafkaGenerationStreamConfig> {

    @Override
    protected Disposable startDisposable(KafkaGenerationStreamConfig config) {
        AloKafkaSender<Long, Long> sender = config.buildKafkaLongSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendValues(config.getTopic(), Function.identity()))
            .doFinally(sender::close)
            .subscribe();
    }
}
