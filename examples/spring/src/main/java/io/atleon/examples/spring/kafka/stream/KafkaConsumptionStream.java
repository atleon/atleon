package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class KafkaConsumptionStream extends AloStream<KafkaConsumptionStreamConfig> {

    @Override
    protected Disposable startDisposable(KafkaConsumptionStreamConfig config) {
        return config.buildKafkaLongReceiver()
            .receiveAloValues(config.getTopic())
            .consume(config.getService()::handleNumber)
            .resubscribeOnError(config.name())
            .subscribe();
    }
}
