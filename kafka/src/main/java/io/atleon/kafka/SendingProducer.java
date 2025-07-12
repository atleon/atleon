package io.atleon.kafka;

import io.atleon.util.Proxying;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A facade around an active {@link Producer} being used for sending.
 *
 * @param <K> The type of keys in records sent by this producer
 * @param <V> The type of values in records sent by this producer
 */
final class SendingProducer<K, V> implements ProducerInvocable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

    private static final Set<String> ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS = new HashSet<>(Arrays.asList(
        "flush",
        "metrics",
        "partitionsFor"
    ));

    private final Producer<K, V> producer;

    private final Producer<K, V> externalProducerProxy;

    private final TaskLoop taskLoop;

    private final ProducerListener producerListener;

    private final boolean sendImmediate;

    private final Duration closeTimeout;

    private final Sinks.Empty<Void> closed = Sinks.unsafe().empty();

    public SendingProducer(KafkaSenderOptions<K, V> options) {
        this.producer = options.createProducer();
        this.externalProducerProxy = Proxying.interfaceMethods(Producer.class, this::invokeProducerFromExternal);
        this.taskLoop = TaskLoop.startWithErrorHandling(options.loadProducerTaskLoopName(), this::onProduceTaskFailure);
        this.producerListener = options.createProducerListener(this);
        this.sendImmediate = options.sendImmediate();
        this.closeTimeout = options.closeTimeout();
    }

    @Override
    public <T> Mono<T> invokeAndGet(Function<? super Producer<?, ?>, T> invocation) {
        if (taskLoop.isSourceOfCurrentThread()) {
            throw new UnsupportedOperationException("ProducerInvocable::invokeAndGet should not be called from Kafka" +
                " worker thread. It should rather be the case that the Producer is directly passed to the call site" +
                " in some way, for example with ProducerListener::onClose.");
        }
        return Mono.create(sink -> taskLoop.schedule(() -> {
            try {
                sink.success(invocation.apply(externalProducerProxy));
            } catch (Throwable e) {
                sink.error(e);
            }
        }));
    }

    public void sendSafely(ProducerRecord<K, V> producerRecord, Callback callback) {
        if (sendImmediate) {
            send(producerRecord, callback);
        } else {
            taskLoop.schedule(() -> send(producerRecord, callback));
        }
    }

    public void closeSafelyAsync() {
        closeSafely().subscribe();
    }

    public Mono<Void> closeSafely() {
        return Mono.create(sink -> taskLoop.schedule(() -> {
            closed.tryEmitEmpty();
            runSafely(() -> producerListener.onClose(externalProducerProxy), "producerListener::onClose");
            runSafely(() -> producer.close(closeTimeout), "producer::close");
            taskLoop.disposeSafely();
            sink.success();
        }));
    }

    public Mono<Void> closed() {
        return closed.asMono();
    }

    private void send(ProducerRecord<K, V> producerRecord, Callback callback) {
        try {
            producer.send(producerRecord, callback);
        } catch (Exception error) {
            onProduceTaskFailure(error);
            callback.onCompletion(null, error);
        }
    }

    private Object invokeProducerFromExternal(Method method, Object[] args) throws ReflectiveOperationException {
        if (!ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS.contains(method.getName())) {
            throw new UnsupportedOperationException("Kafka Producer method is not supported: " + method);
        }

        try {
            return method.invoke(producer, args);
        } catch (RuntimeException e) {
            onProduceTaskFailure(e);
            throw e;
        }
    }

    private void onProduceTaskFailure(Throwable error) {
        if (isFatalProducerException(error)) {
            LOGGER.warn("Encountered fatal producer exception. Producer will be closed.", error);
            closeSafelyAsync();
        } else {
            LOGGER.debug("Encountered non-fatal producer exception.", error);
        }
    }

    private static boolean isFatalProducerException(Throwable error) {
        return error instanceof ProducerFencedException ||
            error instanceof OutOfOrderSequenceException ||
            error instanceof AuthenticationException ||
            error instanceof UnsupportedVersionException ||
            error instanceof UnsupportedForMessageFormatException;
    }

    private static void runSafely(Runnable task, String name) {
        runSafely(task, error -> LOGGER.error("Unexpected failure: name={}", name, error));
    }

    private static void runSafely(Runnable task, Consumer<Throwable> errorHandler) {
        try {
            task.run();
        } catch (Throwable e) {
            errorHandler.accept(e);
        }
    }
}
