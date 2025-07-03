package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

final class PollingSubscriptionFactory<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

    private final KafkaReceiverOptions<K, V> options;

    public PollingSubscriptionFactory(KafkaReceiverOptions<K, V> options) {
        this.options = options;
    }

    public Subscription periodic(
        AssignmentSpec assignmentSpec,
        Subscriber<? super KafkaReceiverRecord<K, V>> subscriber
    ) {
        return new PeriodicCommitPoller(assignmentSpec, subscriber);
    }

    private static boolean isRetriableCommitFailure(Exception exception) {
        return exception instanceof RetriableCommitFailedException
            || exception instanceof RebalanceInProgressException;
    }

    private static <T> Flux<T> mergeGreedily(Collection<? extends Publisher<? extends T>> sources) {
        // Use merge method that takes explicit concurrency so that all provided publishers are
        // immediately (i.e. "greedily") subscribed.
        return Flux.merge(Flux.fromIterable(sources), sources.size());
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private abstract class Poller implements Subscription, ReceivingConsumer.PartitionListener {

        protected final ReceivingConsumer<K, V> receivingConsumer;

        protected final Scheduler auxiliaryScheduler;

        private final Subscriber<? super KafkaReceiverRecord<K, V>> subscriber;

        private final ReceptionListener listener;

        private final int maxPollRecords = options.loadMaxPollRecords();

        private final AtomicInteger freePrefetchCapacity = new AtomicInteger(options.calculateMaxRecordsPrefetch());

        private final AtomicLong requestOutstanding = new AtomicLong(0);

        private final AtomicInteger drainsInProgress = new AtomicInteger(0);

        // This counter doubles as both our publishing state (via polarity: non-negative == ACTIVE,
        // negative == TERMINABLE or TERMINATED) and (when non-negative) our count of activated
        // in-flight records. As such, when this first becomes negative, it means we have entered a
        // TERMINABLE state (error or cancellation). When it is set to Long.MIN_VALUE it means
        // we've reached TERMINATED state and termination has been enqueued.
        private final AtomicLong freeActiveInFlightCapacity = new AtomicLong(options.maxActiveInFlight());

        private final AtomicBoolean pausedDueToBackpressure = new AtomicBoolean(false);

        private final AtomicReference<Throwable> error = new AtomicReference<>();

        private final Map<TopicPartition, ActivePartition> assignments = new ConcurrentHashMap<>();

        private final Set<TopicPartition> externallyPausedPartitions = new CopyOnWriteArraySet<>();

        private final Queue<EmittableRecord<K, V>> emittableRecords = new ConcurrentLinkedQueue<>();

        public Poller(AssignmentSpec assignmentSpec, Subscriber<? super KafkaReceiverRecord<K, V>> subscriber) {
            this.receivingConsumer = new ReceivingConsumer<>(options, this, this::failSafely);
            this.auxiliaryScheduler = options.createAuxiliaryScheduler();
            this.subscriber = subscriber;
            this.listener = options.createReceptionListener();

            receivingConsumer.subscribe(assignmentSpec, this::pollAndDrain);
        }

        @Override
        public final void request(long n) {
            if (n > 0 && requestOutstanding.get() != Long.MAX_VALUE) {
                requestOutstanding.addAndGet(n);
                drain();
            }
        }

        @Override
        public final void cancel() {
            if (freeActiveInFlightCapacity.getAndUpdate(count -> count >= 0 ? -1 : count) >= 0) {
                drain();
            }
        }

        @Override
        public final void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            partitions.forEach(partition -> {
                if (assignments.containsKey(partition)) {
                    throw new IllegalStateException("TopicPartition already assigned: " + partition);
                }

                ActivePartition activePartition = new ActivePartition(partition, options.acknowledgementQueueMode());
                onPartitionActivated(consumer, activePartition);
                listener.onPartitionActivated(partition);

                activePartition.deactivatedRecordCounts()
                    .subscribe(it -> handleRecordsDeactivated(partition, it), this::failSafely);
                assignments.put(partition, activePartition);
            });

            // Newly assigned partitions may either be paused due to external control, or need
            // pausing because there isn't enough outstanding downstream demand (back-pressure).
            Collection<TopicPartition> partitionsToPause = calculatePartitionsToPause(partitions);
            if (!partitionsToPause.isEmpty()) {
                consumer.pause(partitionsToPause);
            }
        }

        @Override
        public final void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            List<ActivePartition> revokedPartitions = removeAssignedPartitions(partitions);

            try {
                onActivePartitionsRevoked(consumer, revokedPartitions);
            } finally {
                revokedPartitions.forEach(it -> listener.onPartitionDeactivated(it.topicPartition()));
            }
        }

        @Override
        public final void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            // No longer assigned, so impossible to commit, and all we can do is clean up.
            List<TopicPartition> lostPartitions = removeAssignedPartitions(partitions).stream()
                .map(ActivePartition::deactivateWithoutGrace)
                .collect(Collectors.collectingAndThen(Collectors.toList(), PollingSubscriptionFactory::mergeGreedily))
                .collectList()
                .block();

            try {
                onActivePartitionsLost(lostPartitions);
            } finally {
                lostPartitions.forEach(listener::onPartitionDeactivated);
            }
        }

        @Override
        public final void onPartitionsExternallyPaused(Collection<TopicPartition> partitions) {
            externallyPausedPartitions.addAll(partitions);
        }

        @Override
        public final void onPartitionsExternallyResumed(Collection<TopicPartition> partitions) {
            externallyPausedPartitions.removeAll(partitions);
        }

        protected abstract void onPartitionActivated(Consumer<?, ?> consumer, ActivePartition partition);

        protected abstract void onActivePartitionsRevoked(Consumer<?, ?> consumer, List<ActivePartition> partitions);

        protected abstract void onActivePartitionsLost(List<TopicPartition> partitions);

        protected final void drain() {
            if (drainsInProgress.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            do {
                // Handle onNext emission and update outstanding capacities.
                long maxToEmit = prepareForActiveEmit();
                long activeEmitted = maxToEmit > 0 ? emitActivatedRecords(maxToEmit) : 0L;
                if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                    freeActiveInFlightCapacity.addAndGet(-activeEmitted);
                }
                if (requestOutstanding.get() != Long.MAX_VALUE) {
                    requestOutstanding.addAndGet(-activeEmitted);
                }

                // Handle termination if now is the time to do so. Don't need CAS here since this
                // is the only place to transition to TERMINATED (Long.MIN_VALUE).
                if (freeActiveInFlightCapacity.get() < 0 && freeActiveInFlightCapacity.get() != Long.MIN_VALUE) {
                    terminateSafely();
                    freeActiveInFlightCapacity.set(Long.MIN_VALUE);
                    receivingConsumer.wakeupSafely();
                }

                missed = drainsInProgress.addAndGet(-missed);
            } while (missed != 0);
        }

        protected long prepareForActiveEmit() {
            return Math.min(freeActiveInFlightCapacity.get(), requestOutstanding.get());
        }

        protected boolean mayContinueActiveEmit() {
            return active();
        }

        protected void handleRecordsActivated(TopicPartition topicPartition, long count) {
            listener.onRecordsActivated(topicPartition, count);
        }

        protected void handleRecordsDeactivated(TopicPartition topicPartition, long count) {
            listener.onRecordsDeactivated(topicPartition, count);
            if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                freeActiveInFlightCapacity.updateAndGet(it -> it >= 0 ? it + count : it);
            }
            drain();
        }

        protected abstract void terminate();

        protected final void failSafely(Throwable failure) {
            if (!active() || !error.compareAndSet(null, failure)) {
                // Failures during termination and failures that don't initiate termination can be
                // safely dropped.
                LOGGER.debug("Ignoring failure during termination", failure);
            } else if (freeActiveInFlightCapacity.getAndUpdate(count -> count >= 0 ? -1 : count) >= 0) {
                // Could be racing with cancellation, but it's not a spec violation if onError
                // emission is concurrent with downstream cancellation.
                drain();
            }
        }

        private void pollAndDrain(Consumer<K, V> consumer) {
            if (freeActiveInFlightCapacity.get() == Long.MIN_VALUE) {
                return;
            }

            ConsumerRecords<K, V> consumerRecords = pollWakeably(consumer);

            int queuedForEmission = 0;
            for (TopicPartition partition : consumerRecords.partitions()) {
                ActivePartition activePartition = assignments.get(partition);
                for (ConsumerRecord<K, V> consumerRecord : consumerRecords.records(partition)) {
                    emittableRecords.add(new EmittableRecord<>(activePartition, consumerRecord));
                    queuedForEmission++;
                }
            }

            if (queuedForEmission > 0) {
                drain();

                // Doing this after draining to avoid possibly-unnecessary pausing
                if (freePrefetchCapacity.addAndGet(-queuedForEmission) < maxPollRecords) {
                    consumer.pause(assignments.keySet());
                    pausedDueToBackpressure.set(true);
                    LOGGER.debug("Assignments paused");
                }
            }

            receivingConsumer.schedule(this::pollAndDrain);
        }

        private ConsumerRecords<K, V> pollWakeably(Consumer<K, V> consumer) {
            try {
                if (pausedDueToBackpressure.get() && freePrefetchCapacity.get() >= maxPollRecords) {
                    consumer.resume(resumeablePartitions());
                    pausedDueToBackpressure.set(false);
                    LOGGER.debug("Assignments resumed");
                }

                return consumer.poll(options.pollTimeout());
            } catch (WakeupException wakeup) {
                LOGGER.debug("Consumer polling woken");
                // Check if this wakeup must have been caused by freeing up capacity, and if so,
                // retry the poll.
                return activelyPausedDueToBackpressure() && freePrefetchCapacity.get() >= maxPollRecords
                    ? pollWakeably(consumer)
                    : ConsumerRecords.empty();
            }
        }

        private List<ActivePartition> removeAssignedPartitions(Collection<TopicPartition> partitions) {
            return partitions.stream().map(assignments::remove).filter(Objects::nonNull).collect(Collectors.toList());
        }

        private Collection<TopicPartition> calculatePartitionsToPause(Collection<TopicPartition> newlyAssigned) {
            if (pausedDueToBackpressure.get()) {
                LOGGER.debug("Rebalance during back-pressure. Pausing...");
                return newlyAssigned;
            } else if (externallyPausedPartitions.isEmpty()) {
                return Collections.emptyList();
            } else {
                LOGGER.debug("Assigned partitions are paused via external control");
                return newlyAssigned.stream().filter(externallyPausedPartitions::contains).collect(Collectors.toList());
            }
        }

        private Collection<TopicPartition> resumeablePartitions() {
            if (externallyPausedPartitions.isEmpty()) {
                return assignments.keySet();
            } else {
                return assignments.keySet().stream()
                    .filter(it -> !externallyPausedPartitions.contains(it))
                    .collect(Collectors.toList());
            }
        }

        private long emitActivatedRecords(long maxToEmit) {
            long emitted = 0;
            EmittableRecord<K, V> emittable;
            while (emitted < maxToEmit && mayContinueActiveEmit() && (emittable = emittableRecords.poll()) != null) {
                if (freePrefetchCapacity.incrementAndGet() == maxPollRecords && activelyPausedDueToBackpressure()) {
                    receivingConsumer.wakeupSafely();
                }

                Optional<KafkaReceiverRecord<K, V>> activated = emittable.activateForProcessing();
                try {
                    TopicPartition topicPartition = emittable.topicPartition();
                    activated.ifPresent(it -> {
                        handleRecordsActivated(topicPartition, 1L);
                        subscriber.onNext(it);
                    });
                } catch (Throwable error) {
                    LOGGER.error("Emission failure (§2.13)", error);
                    failSafely(error);
                }
            }
            return emitted;
        }

        private void terminateSafely() {
            Throwable errorToEmit = error.get();
            if (errorToEmit != null) {
                runSafely(() -> subscriber.onError(errorToEmit), "subscriber::onError §2.13");
                LOGGER.debug("Terminated due to error");
            } else {
                LOGGER.debug("Terminated due to cancel");
            }

            runSafely(this::terminate, "this::terminate");
            receivingConsumer.closeSafely()
                .doOnTerminate(() -> runSafely(listener::close, "listener::close"))
                .doOnTerminate(() -> runSafely(auxiliaryScheduler::dispose, "periodicScheduler::dispose"))
                .subscribe();
        }

        private boolean activelyPausedDueToBackpressure() {
            return pausedDueToBackpressure.get() && active();
        }

        private boolean active() {
            return freeActiveInFlightCapacity.get() >= 0;
        }
    }

    private final class PeriodicCommitPoller extends Poller {

        private final Disposable periodicCommit;

        private final ReceptionSequenceSet sequenceSet = new ReceptionSequenceSet();

        private final Sinks.Empty<Void> disposal = Sinks.empty();

        private final Sinks.Many<CommittableOffset> committableOffsets =
            Sinks.unsafe().many().unicast().onBackpressureError();

        private final SerialQueue<CommittableOffset> committableOffsetQueue =
            SerialQueue.onEmitNext(committableOffsets, this::handleCommittableOffsetEmissionFailure);

        public PeriodicCommitPoller(
            AssignmentSpec assignmentSpec,
            Subscriber<? super KafkaReceiverRecord<K, V>> subscriber
        ) {
            super(assignmentSpec, subscriber);
            this.periodicCommit = committableOffsets.asFlux()
                .windowTimeout(options.commitBatchSize(), options.commitPeriod(), auxiliaryScheduler)
                .concatMap(it -> it.collectMap(CommittableOffset::topicPartition, CommittableOffset::sequencedOffset))
                .subscribe(this::scheduleCommit, this::failSafely);
        }

        @Override
        protected void onPartitionActivated(Consumer<?, ?> consumer, ActivePartition partition) {
            long assignmentSequence = sequenceSet.assigned(partition.topicPartition());
            partition.acknowledgedOffsets()
                .map(it -> new CommittableOffset(it, assignmentSequence))
                .subscribe(committableOffsetQueue::addAndDrain, this::failSafely);
        }

        @Override
        protected void onActivePartitionsRevoked(Consumer<?, ?> consumer, List<ActivePartition> partitions) {
            // Get the latest committable offsets for the partitions that have been revoked (with
            // possible grace period), such that we can commit them synchronously. Although it is
            // possible that these offsets may have been previously committed, it is unlikely that
            // this redundancy is significantly undesirable. Under low load, the overhead of a
            // synchronous commit is not likely to meaningfully degrade already-low throughput. As
            // load increases, so does the likelihood that there will be acknowledged uncommitted
            // offsets, and it becomes more desirable to attempt to honor that progress.
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = partitions.stream()
                .map(it -> it.deactivateLatest(options.revocationGracePeriod(), auxiliaryScheduler, disposal.asMono()))
                .collect(Collectors.collectingAndThen(Collectors.toList(), PollingSubscriptionFactory::mergeGreedily))
                .filter(it -> sequenceSet.getCommitRetry(it.topicPartition()) < options.maxCommitAttempts() - 1)
                .collectMap(AcknowledgedOffset::topicPartition, AcknowledgedOffset::nextOffsetAndMetadata)
                .block();

            try {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            } catch (WakeupException wakeup) {
                // There are two possible causes for a wakeup during partition revocation:
                //   1. Async emission of records freed up capacity for subsequent polling
                //   2. Async termination (without poll invocation consuming wakeup signal)
                // In order to attempt graceful handling of either scenario, we retry the commit.
                // Although it is not likely, it is possible that both scenarios occur (in order),
                // with the first retry attempt being woken by the second scenario. In this case,
                // we retry one LAST time. We then re-emit the original wakeup signal so that the
                // possible parent poll invocation knows about it.
                try {
                    consumer.commitSync(offsetsToCommit);
                } catch (WakeupException __) {
                    consumer.commitSync(offsetsToCommit);
                }
                throw wakeup;
            } finally {
                // Lastly, sanitize sequence counters to account for possible in-flight commits and
                // potential future reassignment (and signal listener about deactivation).
                partitions.forEach(it -> sequenceSet.unassigned(it.topicPartition()));
            }
        }

        @Override
        protected void onActivePartitionsLost(List<TopicPartition> partitions) {
            partitions.forEach(sequenceSet::unassigned);
        }

        @Override
        protected void terminate() {
            // Stop commit scheduling, then force disposal of in-progress deactivations.
            periodicCommit.dispose();
            disposal.tryEmitEmpty();
        }

        private boolean handleCommittableOffsetEmissionFailure(SignalType signalType, Sinks.EmitResult emitResult) {
            // Only reason this should happen is if/when we're terminating with concurrent
            // committable offset emission.
            LOGGER.debug("Committable offset emission failed. Must mean termination: {}-{}", signalType, emitResult);
            return false;
        }

        private void scheduleCommit(Map<TopicPartition, SequencedOffset> assignedOffsets) {
            // Calculate commit sequence numbers eagerly such that invalidation may happen quickly
            // in the case that commits are being rapidly scheduled.
            Map<TopicPartition, Long> commitSequences = assignedOffsets.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), sequenceSet::incrementAndGetCommit));
            receivingConsumer.schedule(consumer -> {
                // Only need to check assignment sequence number on initial commit attempt, because
                // we are now executing on the polling thread, and therefore any assignment changes
                // are guaranteed to visibly increase associated commit sequence counter(s), so we
                // can rely on that for invalidation (i.e. on retries).
                Map<TopicPartition, OffsetAndMetadata> validatedOffsets = assignedOffsets.entrySet().stream()
                    .filter(it -> it.getValue().sequence() == sequenceSet.getAssignment(it.getKey()))
                    .filter(it -> commitSequences.get(it.getKey()) == sequenceSet.getCommit(it.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().offsetAndMetadata()));
                commit(consumer, validatedOffsets, commitSequences);
            });
        }

        private void commit(
            Consumer<K, V> consumer,
            Map<TopicPartition, OffsetAndMetadata> validatedOffsets,
            Map<TopicPartition, Long> commitSequences
        ) {
            if (validatedOffsets.isEmpty()) {
                return;
            }

            consumer.commitAsync(validatedOffsets, (offsets, exception) -> {
                if (exception == null) {
                    offsets.keySet().forEach(sequenceSet::resetCommitRetry);
                    return;
                } else if (!isRetriableCommitFailure(exception)) {
                    failSafely(exception);
                    return;
                }

                int maxRetryCount = offsets.keySet().stream()
                    .mapToInt(sequenceSet::getCommitRetry)
                    .reduce(0, Math::max);
                int remainingAttempts = options.maxCommitAttempts() - maxRetryCount - 1;

                if (remainingAttempts == 0) {
                    failSafely(new KafkaException("Retries exhausted", exception));
                } else {
                    LOGGER.warn("Retrying failed commit (remaining: {}): {}", remainingAttempts, exception.toString());
                    scheduleCommitRetry(validatedOffsets, commitSequences);
                }
            });
        }

        private void scheduleCommitRetry(
            Map<TopicPartition, OffsetAndMetadata> offsets,
            Map<TopicPartition, Long> commitSequences
        ) {
            receivingConsumer.schedule(consumer -> {
                Map<TopicPartition, OffsetAndMetadata> validatedOffsets = offsets.entrySet().stream()
                    .filter(it -> commitSequences.get(it.getKey()) == sequenceSet.getCommit(it.getKey()))
                    .peek(it -> sequenceSet.incrementCommitRetry(it.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                commit(consumer, validatedOffsets, commitSequences);
            });
        }
    }
}
