package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class RabbitMQProcessingStream extends AloStream<RabbitMQProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(RabbitMQProcessingStreamConfig config) {
        return config.buildRabbitMQLongReceiver()
            .receiveAloBodies(config.getQueue())
            .filter(config.getNumbersService()::isPrime)
            .resubscribeOnError(config.name())
            .subscribe(Alo::acknowledge);
    }
}
