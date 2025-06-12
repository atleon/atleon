package io.atleon.kafka;

import io.atleon.util.Collecting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

/**
 * Resource through which management of polling state and polling invocation are delegated. Takes
 * care of managing access to actively assigned partitions (with the ability to map some type of
 * resource to each active assignment), managing pause state, and encapsulating poll
 * quality-of-service, which includes timeout, batch size
 * ({@link org.apache.kafka.clients.consumer.ConsumerConfig#MAX_POLL_RECORDS_CONFIG}), and
 * delegating to a configured {@link PollStrategy} for selecting what partitions to poll per cycle.
 *
 * @param <T> the type of resource maintained per active assignment
 */
final class PollManager<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollManager.class);

    private final PollStrategy pollStrategy;

    private final int maxPollRecords;

    private final Duration pollTimeout;

    private final AtomicBoolean pausedDueToBackpressure = new AtomicBoolean(false);

    private final Set<TopicPartition> forcePaused = ConcurrentHashMap.newKeySet();

    private final Map<TopicPartition, T> assignments = new ConcurrentHashMap<>();

    public PollManager(PollStrategy pollStrategy, int maxPollRecords, Duration pollTimeout) {
        this.pollStrategy = pollStrategy;
        this.maxPollRecords = maxPollRecords;
        this.pollTimeout = pollTimeout;
    }

    public void activateAssigned(
        Consumer<?, ?> consumer,
        Collection<TopicPartition> partitions,
        Function<TopicPartition, T> activator
    ) {
        partitions.forEach(partition -> {
            if (assignments.containsKey(partition)) {
                throw new IllegalStateException("TopicPartition already assigned: " + partition);
            }
            assignments.put(partition, activator.apply(partition));
        });

        // Newly assigned partitions may either be paused due to external control, or need pausing
        // because there isn't enough outstanding downstream demand (back-pressure). There is no
        // need to let the poll strategy know about these partitions, since polling them is
        // prohibited anyway, and even if they were permitted in the past, they MUST have been
        // previously prohibited due to unassignment (or else exception would be thrown above). Do,
        // however, let the strategy know about newly-assigned poll-permitted partitions.
        Map<Boolean, ? extends Collection<TopicPartition>> pollingPermissible = partitionByPollingPermitted(partitions);
        Collection<TopicPartition> prohibited = pollingPermissible.get(false);
        if (!prohibited.isEmpty()) {
            consumer.pause(prohibited);
        }
        Collection<TopicPartition> pollPermittedPartitions = pollingPermissible.get(true);
        if (!pollPermittedPartitions.isEmpty()) {
            pollStrategy.onPollingPermitted(pollPermittedPartitions);
        }
    }

    public Collection<T> unassigned(Collection<TopicPartition> partitions) {
        Map<TopicPartition, T> unassigned = partitions.stream()
            .filter(assignments::containsKey)
            .collect(Collectors.toMap(Function.identity(), assignments::remove));
        pollStrategy.onPollingProhibited(unassigned.keySet());
        return unassigned.values();
    }

    public <K, V> ConsumerRecords<K, V> pollWakeably(
        Consumer<K, V> consumer,
        IntSupplier freeCapacitySupplier,
        BooleanSupplier mayResumeAndPollOnWakeup
    ) {
        boolean shouldBePausedDueToBackpressure = freeCapacitySupplier.getAsInt() < maxPollRecords;
        boolean backpressureStateChange = shouldBePausedDueToBackpressure != pausedDueToBackpressure.get();
        try {
            // Prepare for polling by letting the strategy know if we're about to perform a poll
            // in the absence of a pause due to back-pressure. Else ensure that all assigned
            // partitions are paused if we are transitioning to pause state due to back-pressure,
            // and forego letting the strategy know (since empty preparation would be wasteful).
            if (!shouldBePausedDueToBackpressure) {
                pollStrategy.prepareForPoll(new ConsumerPollSelectionContext(consumer));
            } else if (backpressureStateChange) {
                consumer.pause(assignments.keySet());
            }

            // After successful preparation, ensure back-pressure pause state is updated if needed.
            if (backpressureStateChange) {
                pausedDueToBackpressure.set(shouldBePausedDueToBackpressure);
                LOGGER.debug("{} back-pressure", shouldBePausedDueToBackpressure ? "Paused due to" : "Resumed from");
            }

            return consumer.poll(pollTimeout);
        } catch (WakeupException wakeup) {
            LOGGER.debug("Consumer polling woken");
            // Check if wakeup was likely caused by freeing up capacity, and if so, retry
            return mayResumeAndPollOnWakeup.getAsBoolean() && freeCapacitySupplier.getAsInt() >= maxPollRecords
                ? pollWakeably(consumer, freeCapacitySupplier, mayResumeAndPollOnWakeup)
                : ConsumerRecords.empty();
        }
    }

    public boolean shouldWakeupOnSingularCapacityReclamation(int updatedCapacity) {
        return updatedCapacity == maxPollRecords && pausedDueToBackpressure.get();
    }

    public void forcePause(Collection<TopicPartition> partitions) {
        pollStrategy.onPollingProhibited(partitions);
        forcePaused.addAll(partitions);
    }

    public void allowResumption(Collection<TopicPartition> partitions) {
        pollStrategy.onPollingPermitted(partitions);
        forcePaused.removeAll(partitions);
    }

    public T activated(TopicPartition topicPartition) {
        return assignments.get(topicPartition);
    }

    private Map<Boolean, ? extends Collection<TopicPartition>> partitionByPollingPermitted(
        Collection<TopicPartition> partitions
    ) {
        if (pausedDueToBackpressure.get()) {
            Map<Boolean, Collection<TopicPartition>> result = new HashMap<>();
            result.put(false, partitions);
            result.put(true, Collections.emptyList());
            return result;
        } else if (forcePaused.isEmpty()) {
            Map<Boolean, Collection<TopicPartition>> result = new HashMap<>();
            result.put(false, Collections.emptyList());
            result.put(true, partitions);
            return result;
        } else {
            return partitions.stream().collect(Collectors.partitioningBy(it -> !forcePaused.contains(it)));
        }
    }

    private final class ConsumerPollSelectionContext implements PollSelectionContext {

        private final Consumer<?, ?> consumer;

        private ConsumerPollSelectionContext(Consumer<?, ?> consumer) {
            this.consumer = consumer;
        }

        @Override
        public Map<TopicPartition, Long> currentBatchLag(Set<TopicPartition> partitions, long defaultValue) {
            if (!forcePaused.isEmpty() && partitions.stream().anyMatch(forcePaused::contains)) {
                throw new IllegalArgumentException("Cannot request metadata for prohibited partitions: " + partitions);
            }

            return partitions.stream()
                .collect(Collectors.toMap(Function.identity(), it -> currentBatchLag(it, defaultValue)));
        }

        @Override
        public void selectExclusively(Set<TopicPartition> partitions) {
            if (!forcePaused.isEmpty() && partitions.stream().anyMatch(forcePaused::contains)) {
                throw new IllegalArgumentException("Cannot select prohibited partitions: " + partitions);
            }

            consumer.pause(Collecting.difference(assignments.keySet(), partitions));
            consumer.resume(partitions);
        }

        @Override
        public void selectNaturally() {
            if (forcePaused.isEmpty()) {
                consumer.resume(assignments.keySet());
            } else {
                consumer.pause(forcePaused);
                consumer.resume(Collecting.difference(assignments.keySet(), forcePaused));
            }
        }

        private long currentBatchLag(TopicPartition topicPartition, long defaultValue) {
            long currentLag = consumer.currentLag(topicPartition).orElse(Long.MAX_VALUE);
            return currentLag == Long.MAX_VALUE ? defaultValue : (currentLag / maxPollRecords);
        }
    }
}
