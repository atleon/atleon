package io.atleon.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;

import java.io.IOException;
import java.util.function.Function;

/**
 * Wrapper around a {@link Connection} that is actively being used to publish messages.
 */
final class SendingConnection extends ReactiveConnection {

    private final Pool<ReactiveChannel> channelPool;

    private SendingConnection(Scheduler ioScheduler, Connection connection, RabbitMQSenderOptions options) {
        super(ioScheduler, connection, options.closeTimeout());
        this.channelPool = PoolBuilder.from(createReactiveChannel())
                .destroyHandler(ReactiveChannel::closeGracefully)
                .evictionPredicate((channel, __) -> channel.unusable())
                .sizeBetween(0, options.maxPooledChannels())
                .buildPool();
    }

    public static Mono<SendingConnection> create(RabbitMQSenderOptions options) {
        return create(
                options::createIOScheduler,
                options::createConnection,
                (scheduler, connection) -> new SendingConnection(scheduler, connection, options));
    }

    public <T> Flux<T> useChannel(Function<ReactiveChannel, Publisher<T>> user) {
        return channelPool.withPoolable(user);
    }

    @Override
    protected Channel createChannel() throws IOException {
        Channel channel = super.createChannel();
        channel.confirmSelect();
        return channel;
    }

    @Override
    protected Mono<Void> closeChannelResources() {
        return channelPool.disposeLater();
    }
}
