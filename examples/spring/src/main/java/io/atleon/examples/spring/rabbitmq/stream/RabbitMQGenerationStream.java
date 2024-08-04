package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@AutoConfigureStream
@Profile("!integrationTest")
public class RabbitMQGenerationStream extends SelfConfigurableAloStream {

    private final RabbitMQConfigSource configSource;

    private final Environment environment;

    public RabbitMQGenerationStream(RabbitMQConfigSource exampleRabbitMQConfigSource, Environment environment) {
        this.configSource = exampleRabbitMQConfigSource;
        this.environment = environment;
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
        return configSource.with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName())
            .as(AloRabbitMQSender::create);
    }

    private RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
            environment.getRequiredProperty("stream.rabbitmq.exchange"),
            environment.getRequiredProperty("stream.rabbitmq.input.queue")
        );
    }
}
