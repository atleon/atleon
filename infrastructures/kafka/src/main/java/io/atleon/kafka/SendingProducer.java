package io.atleon.kafka;

import io.atleon.core.TaskLoop;
import io.atleon.util.Proxying;
import io.atleon.util.Publishing;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(SendingProducer.class);

    private static final Set<String> ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS =
            new HashSet<>(Arrays.asList("clientInstanceId", "metrics", "partitionsFor"));

    private final Producer<K, V> producer;

    private final Producer<K, V> externalProducerProxy;

    private final TaskLoop taskLoop;

    private final ProducerListener producerListener;

    private final boolean sendImmediate;

    private final Duration closeTimeout;

    private final Mono<Void> initTransactionsCache;

    private final Sinks.Empty<Void> closed = Sinks.unsafe().empty();

    public SendingProducer(KafkaSenderOptions<K, V> options) {
        this.producer = options.createProducer();
        this.externalProducerProxy = Proxying.interfaceMethods(Producer.class, this::invokeProducerFromExternal);
        this.taskLoop = TaskLoop.start(options.loadProducerTaskLoopName(), this::runProducerTaskSafely);
        this.producerListener = options.createProducerListener(this);
        this.sendImmediate = options.sendImmediate();
        this.closeTimeout = options.closeTimeout();
        // Only cache first success. Until then, let client know about errors
        this.initTransactionsCache = doOnProducer(Producer::initTransactions).as(Publishing::cacheSuccess);
    }

    @Override
    public <T> Mono<T> invokeAndGet(Function<? super Producer<?, ?>, T> invocation) {
        if (taskLoop.isSourceOfCurrentThread()) {
            throw new UnsupportedOperationException("ProducerInvocable::invokeAndGet should not be called from Kafka"
                    + " worker thread. It should rather be the case that the Producer is directly passed to the call site"
                    + " in some way, for example with ProducerListener::onClose.");
        }
        return taskLoop.publish(() -> invocation.apply(externalProducerProxy));
    }

    public Mono<Void> initTransactions() {
        return initTransactionsCache;
    }

    public Mono<Void> beginTransaction() {
        return doOnProducer(Producer::beginTransaction);
    }

    public Mono<Void> sendOffsetsToTransaction(
            Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata metadata) {
        return doOnProducer(it -> it.sendOffsetsToTransaction(offsets, metadata));
    }

    public Mono<Void> commitTransaction() {
        return doOnProducer(Producer::commitTransaction);
    }

    public Mono<Void> abortTransaction() {
        return doOnProducer(Producer::abortTransaction);
    }

    public void sendSafely(Consumer<Runnable> runner, ProducerRecord<K, V> producerRecord, Callback callback) {
        if (sendImmediate) {
            runner.accept(() -> send(runner, producerRecord, callback));
        } else {
            taskLoop.schedule(() -> runner.accept(() -> send(runner, producerRecord, callback)));
        }
    }

    public void closeSafelyAsync() {
        closeSafely().subscribe();
    }

    public Mono<Void> closeSafely() {
        return taskLoop.publish(() -> {
            runSafely(() -> producerListener.onClose(externalProducerProxy), "producerListener::onClose");
            runSafely(() -> producer.close(closeTimeout), "producer::close");
            closed.tryEmitEmpty();
            runSafely(producerListener::close, "producerListener::close");
            taskLoop.disposeSafely();
            return null;
        });
    }

    public Mono<Void> closed() {
        return closed.asMono();
    }

    private Mono<Void> doOnProducer(Consumer<Producer<?, ?>> task) {
        return taskLoop.publish(() -> {
            try {
                task.accept(producer);
                return null;
            } catch (Exception e) {
                onProducerTaskFailure(e);
                throw e;
            }
        });
    }

    private void send(Consumer<Runnable> runner, ProducerRecord<K, V> producerRecord, Callback callback) {
        try {
            runner.accept(() -> producer.send(producerRecord, callback));
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
}
