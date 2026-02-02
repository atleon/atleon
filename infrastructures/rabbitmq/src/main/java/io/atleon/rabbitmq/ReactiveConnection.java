package io.atleon.rabbitmq;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.atleon.util.IORunnable;
import io.atleon.util.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A reactive facade around an active {@link Connection} being used to interact with RabbitMQ.
 */
abstract class ReactiveConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveConnection.class);

    private final Scheduler ioScheduler;

    private final Connection connection;

    private final int closeTimeoutMillis;

    private final Sinks.Empty<Void> closed = Sinks.empty();

    protected ReactiveConnection(Scheduler ioScheduler, Connection connection, Duration closeTimeout) {
        this.ioScheduler = ioScheduler;
        this.connection = connection;
        this.closeTimeoutMillis = Math.toIntExact(closeTimeout.toMillis());

        connection.addShutdownListener(closed::tryEmitError);
    }

    public final void closeSafelyAsync() {
        closeSafely().subscribe();
    }

    public final Mono<Void> closeSafely() {
        Mono<Void> ioClosure = Mono.fromRunnable(() -> {
            runSafely(() -> close(connection, closeTimeoutMillis), "connection::close");
            runSafely(closed::tryEmitEmpty, "closed::tryEmitEmpty");
            runSafely(ioScheduler::dispose, "ioScheduler::dispose");
        });
        return closeChannelResources().then(ioClosure.subscribeOn(ioScheduler));
    }

    public final Mono<Void> closed() {
        return closed.asMono();
    }

    protected final Mono<ReactiveChannel> createReactiveChannel() {
        return getIO(this::createChannel).map(it -> ReactiveChannel.create(it, ioScheduler));
    }

    protected Channel createChannel() throws IOException {
        return connection.createChannel();
    }

    protected abstract Mono<Void> closeChannelResources();

    protected static <T extends ReactiveConnection> Mono<T> create(
            Supplier<Scheduler> ioSchedulerSupplier,
            IOSupplier<Connection> connectionSupplier,
            BiFunction<Scheduler, Connection, T> constructor) {
        return Mono.defer(() -> {
            // Since we're calling with a deferred subscription, it is safe to create a scheduler
            // here with subsequent cleanup if creating a connection is anything but successful.
            // Such cleanup is necessary since it would otherwise be impossible to implement
            // cleanup correctly without successful connection creation.
            Scheduler ioScheduler = ioSchedulerSupplier.get();

            return Mono.fromCallable(connectionSupplier::get)
                    .subscribeOn(ioScheduler)
                    .map(it -> constructor.apply(ioScheduler, it))
                    .doFinally(signalType -> {
                        if (signalType != SignalType.ON_COMPLETE) {
                            ioScheduler.dispose();
                        }
                    });
        });
    }

    private <T> Mono<T> getIO(IOSupplier<T> supplier) {
        return Mono.fromCallable(supplier::get).subscribeOn(ioScheduler);
    }

    private static void close(Connection connection, int timeoutMillis) throws IOException {
        try {
            connection.close(timeoutMillis);
        } catch (AlreadyClosedException e) {
            LOGGER.debug("Connection already closed", e);
        }
    }

    private static void runSafely(IORunnable task, String name) {
        runSafely(task, error -> LOGGER.error("Unexpected failure: name={}", name, error));
    }

    private static void runSafely(IORunnable task, Consumer<Throwable> errorHandler) {
        try {
            task.run();
        } catch (Throwable e) {
            errorHandler.accept(e);
        }
    }
}
