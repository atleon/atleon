package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.ConfigContext;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import reactor.core.Disposable;

@AutoConfigureStream
public class RabbitMQConsumptionStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public RabbitMQConsumptionStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    protected Disposable startDisposable() {
        return buildRabbitMQLongReceiver()
            .receiveAloBodies(getQueue())
            .consume(getService()::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.rabbitmq"))
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    private String getQueue() {
        return context.getProperty("stream.rabbitmq.output.queue");
    }

    private NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
