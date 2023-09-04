package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStream;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.rabbitmq.AloRabbitMQSender;
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
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }
}
