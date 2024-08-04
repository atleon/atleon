package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.core.env.Environment;
import reactor.core.Disposable;

@AutoConfigureStream
public class RabbitMQConsumptionStream extends SelfConfigurableAloStream {

    private final RabbitMQConfigSource configSource;

    private final NumbersService service;

    private final Environment environment;

    public RabbitMQConsumptionStream(
        RabbitMQConfigSource exampleRabbitMQConfigSource,
        NumbersService service,
        Environment environment
    ) {
        this.configSource = exampleRabbitMQConfigSource;
        this.service = service;
        this.environment = environment;
    }

    @Override
    protected Disposable startDisposable() {
        return buildRabbitMQLongReceiver()
            .receiveAloBodies(environment.getRequiredProperty("stream.rabbitmq.output.queue"))
            .consume(service::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        return configSource.with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName())
            .as(AloRabbitMQReceiver::create);
    }
}
