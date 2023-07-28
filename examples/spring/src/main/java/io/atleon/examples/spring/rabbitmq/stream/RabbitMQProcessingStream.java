package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import reactor.core.Disposable;

public class RabbitMQProcessingStream extends AloStream<RabbitMQProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(RabbitMQProcessingStreamConfig config) {
        AloRabbitMQSender<Long> sender = config.buildRabbitMQLongSender();

        return config.buildRabbitMQLongReceiver()
            .receiveAloBodies(config.getInputQueue())
            .filter(config.getService()::isPrime)
            .transform(sender.sendAloBodies(config.buildLongMessageCreator()))
            .resubscribeOnError(config.name())
            .subscribe(Alo::acknowledge);
    }
}
