package io.atleon.kafka;

import io.atleon.core.TaskLoop;
import io.atleon.util.Proxying;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A facade around an active {@link Producer} being used for sending.
 *
 * @param <K> The type of keys in records sent by this producer
 * @param <V> The type of values in records sent by this producer
 */
final class SendingProducer<K, V> implements ProducerInvocable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendingProducer.class);

    private static final Set<String> ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS = new HashSet<>(Arrays.asList(
        "clientInstanceId",
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

    // Only cache first success. Until then, let client know about errors
    private final Mono<Void> initTransactionsCache = cacheSuccess(doOnProducer(Producer::initTransactions));

    public SendingProducer(KafkaSenderOptions<K, V> options) {
        this.producer = options.createProducer();
        this.externalProducerProxy = Proxying.interfaceMethods(Producer.class, this::invokeProducerFromExternal);
        this.taskLoop = TaskLoop.start(options.loadProducerTaskLoopName(), this::runProducerTaskSafely);
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

    public Mono<Void> initTransactions() {
        return initTransactionsCache;
    }

    public Mono<Void> beginTransaction() {
        return doOnProducer(Producer::beginTransaction);
    }

    public Mono<Void> sendOffsetsToTransaction(
        Map<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata metadata
    ) {
        return doOnProducer(it -> it.sendOffsetsToTransaction(offsets, metadata));
    }

    public Mono<Void> commitTransaction() {
        return doOnProducer(Producer::commitTransaction);
    }

    public Mono<Void> abortTransaction() {
        return doOnProducer(Producer::abortTransaction);
    }

    public void sendSafely(ProducerRecord<K, V> producerRecord, BooleanSupplier producePredicate, Callback callback) {
        if (sendImmediate) {
            send(producerRecord, producePredicate, callback);
        } else {
            taskLoop.schedule(() -> send(producerRecord, producePredicate, callback));
        }
    }

    public void closeSafelyAsync() {
        closeSafely().subscribe();
    }

    public Mono<Void> closeSafely() {
        return Mono.create(sink -> taskLoop.schedule(() -> {
            runSafely(() -> producerListener.onClose(externalProducerProxy), "producerListener::onClose");
            runSafely(() -> producer.close(closeTimeout), "producer::close");
            closed.tryEmitEmpty();
            runSafely(producerListener::close, "producerListener::close");
            taskLoop.disposeSafely();
            sink.success();
        }));
    }

    public Mono<Void> closed() {
        return closed.asMono();
    }

    private Mono<Void> doOnProducer(Consumer<Producer<?, ?>> task) {
        return Mono.create(sink -> taskLoop.schedule(() -> {
            try {
                task.accept(producer);
                sink.success();
            } catch (Throwable e) {
                onProducerTaskFailure(e);
                sink.error(e);
            }
        }));
    }

    private void send(ProducerRecord<K, V> producerRecord, BooleanSupplier producePredicate, Callback callback) {
        try {
            if (producePredicate.getAsBoolean()) {
                producer.send(producerRecord, callback);
            }
        } catch (Exception error) {
            onProducerTaskFailure(error);
            callback.onCompletion(null, error);
        }
    }

    private Object invokeProducerFromExternal(Method method, Object[] args) throws ReflectiveOperationException {
        if (!taskLoop.isSourceOfCurrentThread()) {
            throw new UnsupportedOperationException("Kafka Producer must be invoked from task thread");
        }
        if (!ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS.contains(method.getName())) {
            throw new UnsupportedOperationException("Kafka Producer method is not supported: " + method);
        }

        try {
            return method.invoke(producer, args);
        } catch (RuntimeException e) {
            onProducerTaskFailure(e);
            throw e;
        }
    }

    private void runProducerTaskSafely(Runnable task) {
        try {
            task.run();
        } catch (Throwable error) {
            onProducerTaskFailure(error);
        }
    }

    private void onProducerTaskFailure(Throwable error) {
        if (KafkaErrors.isFatalProducerException(error)) {
            LOGGER.warn("Encountered fatal producer exception. Producer will be closed.", error);
            closeSafelyAsync();
        } else {
            LOGGER.debug("Encountered non-fatal producer exception.", error);
        }
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

    private static <T> Mono<T> cacheSuccess(Mono<T> mono) {
        return mono.materialize().cacheInvalidateIf(Signal::hasError).dematerialize();
    }
}
