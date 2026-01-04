package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

@AutoConfigureStream
public class RabbitMQProcessingStream extends SpringAloStream {

    private final RabbitMQConfigSource configSource;

    private final NumbersService service;

    public RabbitMQProcessingStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class);
        this.service = context.getBean(NumbersService.class);
    }

    @Override
    protected Disposable startDisposable() {
        AloRabbitMQSender<Long> sender = buildRabbitMQLongSender();

        return buildRabbitMQLongReceiver()
                .receiveAloBodies(getRequiredProperty("stream.rabbitmq.input.queue"))
                .filter(service::isPrime)
                .transform(sender.sendAloBodies(buildLongMessageCreator()))
                .resubscribeOnError(name())
                .doFinally(sender::close)
                .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        return configSource
                .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName())
                .as(AloRabbitMQReceiver::create);
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        return configSource
                .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName())
                .as(AloRabbitMQSender::create);
    }

    public RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
                getRequiredProperty("stream.rabbitmq.exchange"), getRequiredProperty("stream.rabbitmq.output.queue"));
    }
}
