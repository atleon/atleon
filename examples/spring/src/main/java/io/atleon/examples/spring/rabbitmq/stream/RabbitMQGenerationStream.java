package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.ConfigContext;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@AutoConfigureStream
@Profile("!integrationTest")
public class RabbitMQGenerationStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public RabbitMQGenerationStream(ConfigContext context) {
        this.context = context;
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
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.rabbitmq"))
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.create(configSource);
    }

    private RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
            context.getProperty("stream.rabbitmq.exchange"),
            context.getProperty("stream.rabbitmq.input.queue")
        );
    }
}
