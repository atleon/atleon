package io.atleon.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import io.atleon.util.IORunnable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * A reactive facade around a {@link Channel} being used to interact with RabbitMQ.
 */
final class ReactiveChannel {

    /**
     * <a href="https://www.rabbitmq.com/docs/consumer-priority#how-to-use">Consumer priority</a>
     */
    private static final String PRIORITY_ARGUMENT = "x-priority";

    private final Channel channel;

    private final Scheduler ioScheduler;

    private final CompletableFuture<Void> usefulness = new CompletableFuture<>();

    private final Sinks.Empty<Void> closed = Sinks.empty();

    private ReactiveChannel(Channel channel, Scheduler ioScheduler) {
        this.channel = channel;
        this.ioScheduler = ioScheduler;

        usefulness.whenComplete((__, error) -> close().subscribe());
        channel.addShutdownListener(usefulness::completeExceptionally);
    }

    public static ReactiveChannel create(Channel channel, Scheduler originalIOScheduler) {
        return new ReactiveChannel(channel, Schedulers.single(originalIOScheduler));
    }

    public <T extends ConfirmListener & ReturnListener & ShutdownListener> void addPublishListener(T listener) {
        channel.addConfirmListener(listener);
        channel.addReturnListener(listener);
        channel.addShutdownListener(listener);
    }

    public <T extends ConfirmListener & ReturnListener & ShutdownListener> void removePublishListener(T listener) {
        channel.removeConfirmListener(listener);
        channel.removeReturnListener(listener);
        channel.removeShutdownListener(listener);
    }

    public Mono<Void> basicPublish(Consumer<Runnable> runner, LongFunction<BasicPublishArguments> argsFactory) {
        return runIO(() -> runner.accept(() -> {
            long sequencedDeliveryTag = channel.getNextPublishSeqNo();
            BasicPublishArguments args = argsFactory.apply(sequencedDeliveryTag);
            try {
                channel.basicPublish(args.exchange(), args.routingKey(), false, false, args.properties(), args.body());
            } catch (IOException e) {
                throw new PublishException(sequencedDeliveryTag, e);
            }
        }));
    }

    public Mono<Void> basicConsume(BasicConsumeArguments args, com.rabbitmq.client.Consumer consumer) {
        return runIO(() -> {
            Map<String, Object> arguments = args.priority()
                    .<Map<String, Object>>map(it -> Collections.singletonMap(PRIORITY_ARGUMENT, it))
                    .orElse(Collections.emptyMap());
            channel.basicConsume(args.queue(), false, args.consumerTag(), false, false, arguments, consumer);
        });
    }

    public Mono<Void> basicAck(long deliveryTag, boolean multiple) {
        return runIO(() -> channel.basicAck(deliveryTag, multiple));
    }

    public Mono<Void> basicNack(long deliveryTag, boolean multiple, boolean requeue) {
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
