package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStream;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

public class RabbitMQGeneration extends AloStream<RabbitMQGenerationConfig> {

    @Override
    protected Disposable startDisposable(RabbitMQGenerationConfig config) {
        AloRabbitMQSender<Long> sender = config.buildRabbitMQLongSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendBodies(DefaultRabbitMQMessageCreator.minimalBasic(config.getExchange())))
            .subscribe();
    }
}
