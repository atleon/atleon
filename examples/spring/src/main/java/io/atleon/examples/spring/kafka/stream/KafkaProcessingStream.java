package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class KafkaProcessingStream extends AloStream<KafkaProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(KafkaProcessingStreamConfig config) {
        return config.buildKafkaLongReceiver()
            .receiveAloValues(config.getTopic())
            .filter(config.getNumbersService()::isPrime)
            .resubscribeOnError(config.name())
            .subscribe(Alo::acknowledge);
    }
}
