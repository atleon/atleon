package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

@AutoConfigureStream
public class RabbitMQConsumptionStream extends SpringAloStream {

    private final RabbitMQConfigSource configSource;

    private final NumbersService service;

    public RabbitMQConsumptionStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class);
        this.service = context.getBean(NumbersService.class);
    }

    @Override
    protected Disposable startDisposable() {
        return buildRabbitMQLongReceiver()
            .receiveAloBodies(getRequiredProperty("stream.rabbitmq.output.queue"))
            .consume(service::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        return configSource.with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName())
            .as(AloRabbitMQReceiver::create);
    }
}
