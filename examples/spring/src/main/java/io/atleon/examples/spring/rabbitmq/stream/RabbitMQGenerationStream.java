package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStream;
import io.atleon.rabbitmq.AloRabbitMQSender;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

public class RabbitMQGenerationStream extends AloStream<RabbitMQGenerationStreamConfig> {

    @Override
    protected Disposable startDisposable(RabbitMQGenerationStreamConfig config) {
        AloRabbitMQSender<Long> sender = config.buildRabbitMQLongSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendBodies(config.buildMessageCreator()))
            .doFinally(sender::close)
            .subscribe();
    }
}
