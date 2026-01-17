package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@AutoConfigureStream
@Profile("!integrationTest")
public class RabbitMQGenerationStream extends SpringAloStream {

    private final RabbitMQConfigSource configSource;

    public RabbitMQGenerationStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class);
    }

    @Override
    protected Disposable startDisposable() {
        AloRabbitMQSender<Long> sender = buildRabbitMQLongSender();

        return Flux.interval(Duration.ofMillis(100))
                .transform(sender.sendBodies(buildLongMessageCreator()))
                .doFinally(sender::close)
                .subscribe();
    }

    private AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        return configSource
                .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName())
                .as(AloRabbitMQSender::create);
    }

    private RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
                getRequiredProperty("stream.rabbitmq.exchange"), getRequiredProperty("stream.rabbitmq.input.queue"));
    }
}
