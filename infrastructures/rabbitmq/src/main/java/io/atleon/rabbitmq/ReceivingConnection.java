package io.atleon.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;

final class ReceivingConnection extends ReactiveConnection {

    private final int prefetch;

    private final String consumerTag;

    private final Mono<ReactiveChannel> channel;

    private ReceivingConnection(Scheduler ioScheduler, Connection connection, RabbitMQReceiverOptions options) {
        super(ioScheduler, connection, options.closeTimeout());
        this.prefetch = options.prefetch();
        this.consumerTag = options.consumerTag();
        this.channel = createReactiveChannel().cache();
    }

    public static Mono<ReceivingConnection> create(RabbitMQReceiverOptions options) {
        return create(
                options::createIOScheduler,
                options::createConnection,
                (scheduler, connection) -> new ReceivingConnection(scheduler, connection, options));
    }

    public Mono<Void> consume(String queue, Consumer consumer) {
        ConsumeArguments args = new ConsumeArguments(queue, consumerTag);
        return channel.flatMap(it -> it.consume(args, consumer));
    }

    public Mono<Void> ack(long deliveryTag) {
        return channel.flatMap(it -> it.ack(deliveryTag, false));
    }

    public Mono<Void> reject(long deliveryTag) {
        return channel.flatMap(it -> it.nack(deliveryTag, false, false));
    }

    public Mono<Void> requeue(long deliveryTag) {
        return channel.flatMap(it -> it.nack(deliveryTag, false, true));
    }

    @Override
    protected Channel createChannel() throws IOException {
        Channel channel = super.createChannel();
        channel.basicQos(prefetch);
        return channel;
    }

    @Override
    protected Mono<Void> closeChannelResources() {
        return Mono.empty();
    }
}
