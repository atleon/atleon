package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class RabbitMQConsumptionStream extends AloStream<RabbitMQConsumptionStreamConfig> {

    @Override
    protected Disposable startDisposable(RabbitMQConsumptionStreamConfig config) {
        return config.buildRabbitMQLongReceiver()
            .receiveAloBodies(config.getQueue())
            .consume(config.getService()::handleNumber)
            .resubscribeOnError(config.name())
            .subscribe();
    }
}
