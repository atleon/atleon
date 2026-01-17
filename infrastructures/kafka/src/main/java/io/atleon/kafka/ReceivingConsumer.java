package io.atleon.kafka;

import io.atleon.core.TaskLoop;
import io.atleon.util.Proxying;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
            "clientInstanceId",
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
            "subscription"));

    private final Consumer<K, V> consumer;

    private final Consumer<K, V> externalConsumerProxy;

    private final PartitionListener partitionListener;

    private final TaskLoop taskLoop;

    private final ConsumerListener consumerListener;

    private final Duration closeTimeout;

    private final Set<TopicPartition> validAssignment = new HashSet<>();

    public ReceivingConsumer(
            KafkaReceiverOptions<K, V> options,
            PartitionListener partitionListener,
            java.util.function.Consumer<Throwable> errorHandler) {
        this.consumer = options.createConsumer();
        this.externalConsumerProxy = Proxying.interfaceMethods(Consumer.class, this::invokeConsumerFromExternal);
        this.partitionListener = partitionListener;
        this.taskLoop = TaskLoop.start(options.loadConsumerTaskLoopName(), it -> runSafely(it, errorHandler));
        this.consumerListener = options.createConsumerListener(this);
        this.closeTimeout = options.closeTimeout();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        validAssignment.removeAll(partitions);
        LOGGER.info("Notifying listeners of lost partitions={}", partitions);
        onRebalance(partitionListener::onPartitionsLost, consumerListener::onPartitionsLost, partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        validAssignment.removeAll(partitions);
        LOGGER.info("Notifying listeners of revoked partitions={}", partitions);
        onRebalance(partitionListener::onPartitionsRevoked, consumerListener::onPartitionsRevoked, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        validAssignment.addAll(partitions);
        Collection<TopicPartition> assignment = consumer.assignment();

        // Detect a known issue where partition (re)assignment is not properly handled by the
        // underlying Kafka Consumer implementation, and fail fatally if detected. This condition
        // appears to be possible when paused partitions are revoked and reassigned in an
        // interrupted rebalance cycle (due to wakeup), and can otherwise lead to non-consumption
        // of assigned partitions, and/or skipping of records. For more info:
        // - https://github.com/atleon/atleon/issues/422
        // - https://github.com/atleon/atleon/issues/445
        Collection<TopicPartition> invalidAssignment =
                assignment.stream().filter(it -> !validAssignment.contains(it)).collect(Collectors.toList());
        if (!invalidAssignment.isEmpty()) {
            throw new IllegalStateException("After rebalancing, there are partitions currently assigned that are"
                    + " missing from the onPartitionsAssigned callback. These partitions have either never been assigned to"
                    + " this consumer, or (more likely) were just recently revoked. This is suspected to be a bug in the"
                    + " Kafka Consumer implementation that could lead to skipped records due to improper position"
                    + " management on partition reassignment. For future debugging, this condition tends to be correlated"
                    + " with paused partitions being revoked, followed by interruption to the rebalance process due to"
                    + " invoking Consumer.wakeup(). The affected partitions are: "
                    + invalidAssignment);
        }

        if (partitions.size() != assignment.size()) {
            LOGGER.info("Assignment appears incremental for assignment={}", assignment);
        }

        LOGGER.info("Notifying listeners of assigned partitions={}", partitions);
        onRebalance(partitionListener::onPartitionsAssigned, consumerListener::onPartitionsAssigned, partitions);
    }

    @Override
    public <T> Mono<T> invokeAndGet(Function<? super Consumer<?, ?>, T> invocation) {
        if (taskLoop.isSourceOfCurrentThread()) {
            throw new UnsupportedOperationException("ConsumerInvocable::invokeAndGet must not be called from the"
                    + " polling thread. It should rather be the case that the Consumer is directly passed to the call site"
                    + " in some way, for example with ConsumerListener::onPartitionsAssigned.");
        }
        return taskLoop.publish(() -> invocation.apply(externalConsumerProxy));
    }

    public void init(ConsumptionSpec consumptionSpec, java.util.function.Consumer<Consumer<K, V>> andThen) {
        taskLoop.schedule(() -> {
            consumptionSpec.onInit(consumer, this);
            andThen.accept(consumer);
        });
    }

    public Mono<Void> closeSafely(ConsumptionSpec consumptionSpec) {
        return taskLoop.publish(() -> {
            runSafely(() -> consumptionSpec.onClose(consumer, this), "consumptionSpec::onClose");
            runSafely(() -> consumerListener.onClose(externalConsumerProxy), "consumerListener::onClose");
            runSafely(() -> consumer.close(closeTimeout), "consumer::close");
            runSafely(consumerListener::close, "consumerListener::close");
            taskLoop.disposeSafely();
            return null;
        });
    }

    public void wakeupSafely() {
        // It does not make sense to call wakeup if we know we're executing on the only thread that
        // could execute a long-running call (i.e. poll), so avoid unnecessary interrupt.
        if (!taskLoop.isSourceOfCurrentThread()) {
            runSafely(consumer::wakeup, "consumer::wakeup");
        }
    }

    public void schedule(java.util.function.Consumer<? super Consumer<K, V>> task) {
        taskLoop.schedule(() -> task.accept(consumer));
    }

    private void onRebalance(
            BiConsumer<Consumer<?, ?>, Collection<TopicPartition>> internalHandler,
            BiConsumer<Consumer<?, ?>, Collection<TopicPartition>> externalHandler,
            Collection<TopicPartition> partitions) {
        internalHandler.accept(consumer, partitions);

        // Not wrapping with try-catch. If user does something naughty, let the error be emitted.
        externalHandler.accept(externalConsumerProxy, partitions);
    }

    private Object invokeConsumerFromExternal(Method method, Object[] args) throws ReflectiveOperationException {
        if (!taskLoop.isSourceOfCurrentThread()) {
            throw new UnsupportedOperationException("Kafka Consumer must be invoked from polling thread");
        }
        if (!ALLOWED_EXTERNAL_CONSUMER_INVOCATIONS.contains(method.getName())) {
            throw new UnsupportedOperationException("Kafka Consumer method is not supported: " + method);
        }

        if (method.getName().equals("pause")) {
            partitionListener.onExternalPartitionsPauseRequested((Collection<TopicPartition>) args[0]);
            return null;
        } else if (method.getName().equals("resume")) {
            partitionListener.onExternalPartitionsResumeRequested((Collection<TopicPartition>) args[0]);
            return null;
        } else {
            return method.invoke(consumer, args);
        }
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

        void onExternalPartitionsPauseRequested(Collection<TopicPartition> partitions);

        void onExternalPartitionsResumeRequested(Collection<TopicPartition> partitions);
    }
}
