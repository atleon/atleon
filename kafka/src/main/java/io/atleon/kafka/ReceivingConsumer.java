package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import io.atleon.util.Proxying;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A facade around an active {@link Consumer} being used for reception.
 *
 * @param <K> The type of keys in records polled by this consumer
 * @param <V> The type of values in records polled by this consumer
 */
final class ReceivingConsumer<K, V> implements ConsumerRebalanceListener, ConsumerInvocable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceivingConsumer.class);

    private static final Set<String> ALLOWED_EXTERNAL_CONSUMER_INVOCATIONS = new HashSet<>(Arrays.asList(
        "assignment",
        "beginningOffsets",
        "committed",
        "currentLag",
        "endOffsets",
        "groupMetadata",
        "listTopics",
        "metrics",
        "offsetsForTimes",
        "partitionsFor",
        "pause",
        "paused",
        "position",
        "resume",
        "seek",
        "seekToBeginning",
        "seekToEnd",
        "subscription"
    ));

    private final Consumer<K, V> consumer;

    private final Consumer<K, V> externalConsumerProxy;

    private final PartitionListener partitionListener;

    private final Scheduler taskScheduler;

    private final Disposable taskLoop;

    private final ConsumerListener consumerListener;

    private final Duration closeTimeout;

    private final Sinks.Many<Runnable> tasks = Sinks.unsafe().many().unicast().onBackpressureError();

    private final SerialQueue<Runnable> taskQueue = SerialQueue.onEmitNext(tasks);

    public ReceivingConsumer(
        KafkaReceiverOptions<K, V> options,
        PartitionListener partitionListener,
        java.util.function.Consumer<Throwable> errorHandler
    ) {
        this.consumer = options.createConsumer();
        this.externalConsumerProxy = Proxying.interfaceMethods(Consumer.class, this::invokeConsumerFromExternal);
        this.partitionListener = partitionListener;
        this.taskScheduler = KafkaSchedulers.newSingleForReception("task", options.loadClientId());
        this.taskLoop = tasks.asFlux()
            .publishOn(taskScheduler, Integer.MAX_VALUE)
            .subscribe(it -> runSafely(it, errorHandler));
        this.consumerListener = options.createConsumerListener(this);
        this.closeTimeout = options.closeTimeout();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        onRebalance(partitionListener::onPartitionsLost, consumerListener::onPartitionsLost, partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        onRebalance(partitionListener::onPartitionsRevoked, consumerListener::onPartitionsRevoked, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        onRebalance(partitionListener::onPartitionsAssigned, consumerListener::onPartitionsAssigned, partitions);
    }

    @Override
    public <T> Mono<T> invokeAndGet(Function<? super Consumer<?, ?>, T> invocation) {
        if (KafkaSchedulers.isCurrentThreadFromKafka()) {
            throw new UnsupportedOperationException("ConsumerInvocable::invokeAndGet must not be called from the" +
                " polling thread. It should rather be the case that the Consumer is directly passed to the call site" +
                " in some way, for example with ConsumerListener::onPartitionsAssigned.");
        }
        return Mono.create(sink -> schedule(() -> {
            try {
                sink.success(invocation.apply(externalConsumerProxy));
            } catch (Throwable e) {
                sink.error(e);
            }
        }));
    }

    public void subscribe(AssignmentSpec assignmentSpec, java.util.function.Consumer<Consumer<K, V>> andThen) {
        schedule(() -> {
            assignmentSpec.apply(consumer, this);
            andThen.accept(consumer);
        });
    }

    public Mono<Void> closeSafely() {
        return Mono.create(sink -> schedule(() -> {
            runSafely(() -> consumerListener.onClose(consumer), "consumerListener::onClose");
            runSafely(() -> consumer.close(closeTimeout), "consumer::close");
            runSafely(taskLoop::dispose, "taskLoop::dispose");
            runSafely(taskScheduler::dispose, "taskScheduler::dispose");
            sink.success();
        }));
    }

    public void wakeupSafely() {
        // It does not make sense to call wakeup if we know we're executing on the only thread that
        // could execute a long-running call (i.e. poll), so avoid unnecessary interrupt.
        if (!KafkaSchedulers.isCurrentThreadFromKafka()) {
            runSafely(consumer::wakeup, "consumer::wakeup");
        }
    }

    public void schedule(java.util.function.Consumer<Consumer<K, V>> task) {
        schedule(() -> task.accept(consumer));
    }

    private void schedule(Runnable task) {
        taskQueue.addAndDrain(task);
    }

    private void onRebalance(
        BiConsumer<Consumer<?, ?>, Collection<TopicPartition>> internalHandler,
        BiConsumer<Consumer<?, ?>, Collection<TopicPartition>> externalHandler,
        Collection<TopicPartition> partitions
    ) {
        internalHandler.accept(consumer, partitions);

        // Not wrapping with try-catch. If user does something naughty, let the error be emitted.
        externalHandler.accept(externalConsumerProxy, partitions);
    }

    private Object invokeConsumerFromExternal(Method method, Object[] args) throws ReflectiveOperationException {
        if (!KafkaSchedulers.isCurrentThreadFromKafka()) {
            throw new UnsupportedOperationException("Kafka Consumer must be invoked from polling thread");
        }
        if (!ALLOWED_EXTERNAL_CONSUMER_INVOCATIONS.contains(method.getName())) {
            throw new UnsupportedOperationException("Kafka Consumer method is not supported: " + method);
        }

        Object result = method.invoke(consumer, args);
        if (method.getName().equals("pause")) {
            partitionListener.onPartitionsExternallyPaused((Collection<TopicPartition>) args[0]);
        } else if (method.getName().equals("resume")) {
            partitionListener.onPartitionsExternallyResumed((Collection<TopicPartition>) args[0]);
        }
        return result;
    }

    private static void runSafely(Runnable task, String name) {
        runSafely(task, error -> LOGGER.error("Unexpected failure: name={}", name, error));
    }

    private static void runSafely(Runnable task, java.util.function.Consumer<Throwable> errorHandler) {
        try {
            task.run();
        } catch (Throwable e) {
            errorHandler.accept(e);
        }
    }

    public interface PartitionListener {

        void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions);

        void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions);

        void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions);

        void onPartitionsExternallyPaused(Collection<TopicPartition> partitions);

        void onPartitionsExternallyResumed(Collection<TopicPartition> partitions);
    }
}
