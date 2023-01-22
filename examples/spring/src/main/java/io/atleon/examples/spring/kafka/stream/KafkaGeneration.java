package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStream;
import io.atleon.kafka.AloKafkaSender;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

public class KafkaGeneration extends AloStream<KafkaGenerationConfig> {

    @Override
    protected Disposable startDisposable(KafkaGenerationConfig config) {
        AloKafkaSender<Long, Long> sender = config.buildKafkaLongSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendValues(config.getTopic(), Function.identity()))
            .doFinally(sender::close)
            .subscribe();
    }
}
