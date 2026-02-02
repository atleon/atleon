package io.atleon.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.atleon.util.IORunnable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Wrapper around a RabbitMQ Channel that provides reactive internal interface for working with a
 * {@link Channel}.
 */
final class ReactiveChannel implements ShutdownListener {

    private final Channel channel;

    private final Scheduler ioScheduler;

    private final CompletableFuture<Void> usefulness = new CompletableFuture<>();

    private final Sinks.Empty<Void> closed = Sinks.empty();

    private ReactiveChannel(Channel channel, Scheduler ioScheduler) {
        this.channel = channel;
        this.ioScheduler = ioScheduler;

        usefulness.whenComplete((__, error) -> close().subscribe());
        channel.addShutdownListener(this);
    }

    public static ReactiveChannel create(Channel channel, Scheduler originalIOScheduler) {
        return new ReactiveChannel(channel, Schedulers.single(originalIOScheduler));
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        usefulness.completeExceptionally(cause);
    }

    public void addConfirmListener(ConfirmListener listener) {
        channel.addConfirmListener(listener);
    }

    public void removeConfirmListener(ConfirmListener listener) {
        channel.removeConfirmListener(listener);
    }

    public void addReturnListener(ReturnListener listener) {
        channel.addReturnListener(listener);
    }

    public void removeReturnListener(ReturnListener listener) {
        channel.removeReturnListener(listener);
    }

    public void addShutdownListener(ShutdownListener listener) {
        channel.addShutdownListener(listener);
    }

    public void removeShutdownListener(ShutdownListener listener) {
        channel.removeShutdownListener(listener);
    }

    public Mono<Void> publish(Consumer<Runnable> runner, LongFunction<PublishArguments> argsFactory) {
        return runIO(() -> runner.accept(() -> {
            long sequencedDeliveryTag = channel.getNextPublishSeqNo();
            PublishArguments args = argsFactory.apply(sequencedDeliveryTag);
            try {
                channel.basicPublish(args.exchange(), args.routingKey(), false, false, args.properties(), args.body());
            } catch (IOException e) {
                throw new PublishException(sequencedDeliveryTag, e);
            }
        }));
    }

    public Mono<Void> consume(ConsumeArguments args, com.rabbitmq.client.Consumer consumer) {
        return runIO(() -> channel.basicConsume(
                args.queue(), false, args.consumerTag(), false, false, Collections.emptyMap(), consumer));
    }

    public Mono<Void> ack(long deliveryTag, boolean multiple) {
        return runIO(() -> channel.basicAck(deliveryTag, multiple));
    }

    public Mono<Void> nack(long deliveryTag, boolean multiple, boolean requeue) {
        return runIO(() -> channel.basicNack(deliveryTag, multiple, requeue));
    }

    public Mono<Void> closeGracefully() {
        return Mono.fromRunnable(() -> usefulness.complete(null)).then(closed.asMono());
    }

    public boolean unusable() {
        return usefulness.isDone();
    }

    private Mono<Void> close() {
        return runIO(() -> {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (TimeoutException e) {
                throw new IOException("Timeout on Channel closure", e);
            } finally {
                closed.tryEmitEmpty();
                ioScheduler.dispose();
            }
        });
    }

    private Mono<Void> runIO(IORunnable runnable) {
        Mono<Void> call = Mono.fromCallable(() -> {
            runnable.run();
            return null;
        });
        return call.subscribeOn(ioScheduler);
    }
}
