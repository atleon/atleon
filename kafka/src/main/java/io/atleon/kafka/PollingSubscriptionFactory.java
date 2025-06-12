package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import io.atleon.core.ShouldBeTerminatedEmitFailureHandler;
import io.atleon.util.Consuming;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
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

    public Subscription periodicCommit(
        AssignmentSpec assignmentSpec,
        Subscriber<? super KafkaReceiverRecord<K, V>> subscriber
    ) {
        return new PeriodicCommitPoller(assignmentSpec, subscriber);
    }

    public Subscription transactional(
        KafkaTxManager txManager,
        AssignmentSpec assignmentSpec,
        Subscriber<? super KafkaReceiverRecord<K, V>> subscriber
    ) {
        return new TransactionalPoller(txManager, assignmentSpec, subscriber);
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private static <T> Flux<T> mergeGreedily(Collection<? extends Publisher<? extends T>> sources) {
        // Use merge method that takes explicit concurrency so that all provided publishers are
        // immediately (i.e. "greedily") subscribed.
        return Flux.merge(Flux.fromIterable(sources), sources.size());
    }

    private abstract class Poller implements Subscription, ReceivingConsumer.PartitionListener {

        protected final ReceivingConsumer<K, V> receivingConsumer;

        protected final Scheduler auxiliaryScheduler;

        private final Subscriber<? super KafkaReceiverRecord<K, V>> subscriber;

        private final ReceptionListener listener;

        private final int maxPollRecords = options.loadMaxPollRecords();

        private final AtomicInteger freePrefetchCapacity = new AtomicInteger(options.calculateMaxRecordsPrefetch());

        private final AtomicLong requested = new AtomicLong(0);

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

        private final Queue<EmittableRecord<K, V>> emittableRecords = new ConcurrentLinkedQueue<>();

        private final Set<TopicPartition> externallyPausedPartitions = ConcurrentHashMap.newKeySet();

        public Poller(AssignmentSpec assignmentSpec, Subscriber<? super KafkaReceiverRecord<K, V>> subscriber) {
            this.receivingConsumer = new ReceivingConsumer<>(options, this, this::failSafely);
            this.auxiliaryScheduler = options.createAuxiliaryScheduler();
            this.subscriber = subscriber;
            this.listener = options.createReceptionListener();

            receivingConsumer.subscribe(assignmentSpec, this::pollAndDrain);
        }

        @Override
        public final void request(long n) {
            if (Operators.validate(n) && requested.getAndUpdate(it -> Operators.addCap(it, n)) == 0) {
                drain();
            }
        }

        @Override
        public final void cancel() {
            if (freeActiveInFlightCapacity.getAndUpdate(it -> it >= 0 ? -1 : it) >= 0) {
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
            Map<TopicPartition, Long> lostPartitionRecordCounts = removeAssignedPartitions(partitions).stream()
                .map(it -> it.deactivateForcefully().map(recordCount -> Tuples.of(it.topicPartition(), recordCount)))
                .collect(Collectors.collectingAndThen(Collectors.toList(), PollingSubscriptionFactory::mergeGreedily))
                .collectMap(Tuple2::getT1, Tuple2::getT2)
                .block();

            try {
                onActivePartitionsLost(lostPartitionRecordCounts);
            } finally {
                lostPartitionRecordCounts.keySet().forEach(listener::onPartitionDeactivated);
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

        protected abstract void onActivePartitionsLost(Map<TopicPartition, Long> lostPartitionRecordCounts);

        protected final void drain() {
            if (drainsInProgress.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            do {
                // Handle onNext emission, update outstanding capacities, and re-trigger drain loop
                // if we exhaust whatever resource first limits emission. This makes it such that
                // we don't need to invoke drain every time a possibly-limiting resource is updated
                // (request, active cap, etc.), unless/until it is updated from (or to) zero.
                long maxToEmit = prepareForActiveEmit();
                if (maxToEmit > 0) {
                    long activeEmitted = emitActivatedRecords(maxToEmit);
                    if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                        freeActiveInFlightCapacity.addAndGet(-activeEmitted);
                    }
                    if (requested.get() != Long.MAX_VALUE) {
                        requested.addAndGet(-activeEmitted);
                    }
                    if (maxToEmit == activeEmitted) {
                        drainsInProgress.incrementAndGet();
                    }
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
            return emittableRecords.isEmpty() ? 0L : Math.min(freeActiveInFlightCapacity.get(), requested.get());
        }

        protected boolean mayContinueActiveEmit() {
            return active();
        }

        protected void handleRecordActivated(TopicPartition topicPartition) {
            listener.onRecordsActivated(topicPartition, 1L);
        }

        protected boolean handleRecordsDeactivated(TopicPartition topicPartition, long count) {
            listener.onRecordsDeactivated(topicPartition, count);
            if (freeActiveInFlightCapacity.getAndUpdate(it -> it >= 0 && it != Long.MAX_VALUE ? it + count : it) == 0) {
                drain();
                return true;
            } else {
                return false;
            }
        }

        protected abstract void terminate();

        protected final void failSafely(Throwable failure) {
            if (!active() || !error.compareAndSet(null, failure)) {
                // Failures during termination and failures that don't initiate termination can be
                // safely dropped.
                LOGGER.info("Ignoring failure during termination", failure);
            } else if (freeActiveInFlightCapacity.getAndUpdate(it -> it >= 0 ? -1 : it) >= 0) {
                // Could be racing with cancellation, but it's not a spec violation if onError
                // emission is concurrent with downstream cancellation.
                drain();
            }
        }

        private List<ActivePartition> removeAssignedPartitions(Collection<TopicPartition> partitions) {
            return partitions.stream().map(assignments::remove).filter(Objects::nonNull).collect(Collectors.toList());
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
                // Check if wakeup must have been caused by freeing up capacity, and if so, retry
                return activelyPausedDueToBackpressure() && freePrefetchCapacity.get() >= maxPollRecords
                    ? pollWakeably(consumer)
                    : ConsumerRecords.empty();
            }
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

                KafkaReceiverRecord<K, V> activated = emittable.activateForProcessing().orElse(null);
                try {
                    if (activated != null) {
                        handleRecordActivated(emittable.topicPartition());
                        subscriber.onNext(activated);
                        emitted++;
                    }
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

        // Only reason emission failure could/should happen is if/when we're terminating with
        // concurrent committable offset emission, hence provided failure handler.
        private final SerialQueue<CommittableOffset> committableOffsetQueue =
            SerialQueue.onEmitNext(committableOffsets, new ShouldBeTerminatedEmitFailureHandler(LOGGER));

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
            java.util.function.Consumer<AcknowledgedOffset> acknowledgedOffsetHandler = options.commitlessOffsets()
                ? Consuming.noOp()
                : it -> committableOffsetQueue.addAndDrain(new CommittableOffset(it, assignmentSequence));
            partition.acknowledgedOffsets().subscribe(acknowledgedOffsetHandler, this::failSafely);
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
                if (!offsetsToCommit.isEmpty() && !options.commitlessOffsets()) {
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
        protected void onActivePartitionsLost(Map<TopicPartition, Long> lostPartitionRecordCounts) {
            lostPartitionRecordCounts.keySet().forEach(sequenceSet::unassigned);
        }

        @Override
        protected void terminate() {
            // Stop commit scheduling, then force disposal of in-progress deactivations.
            periodicCommit.dispose();
            disposal.tryEmitEmpty();
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
                } else if (!KafkaErrors.isRetriableCommitFailure(exception)) {
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
                    scheduleCommitRetry(offsets, commitSequences);
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

    private final class TransactionalPoller extends Poller {

        private final KafkaTxManager txManager;

        private final Disposable transactionalOffsetsSend;

        // This keeps track of our transactional state as a representation of its (remaining)
        // capacity. Special negative values are used to represent non-OPENED states. When
        // positive, this indicates the number of remaining records that may be emitted in a
        // currently open transaction (configurable via commitBatchSize). When zero, it indicates
        // we are currently waiting for all previously emitted records in an open transaction to be
        // deactivated (finish processing), before sending offsets and/or committing the
        // transaction.
        private final AtomicLong capState = new AtomicLong(TxCapStates.UNOPENED);

        private final AtomicLong activeInTransaction = new AtomicLong(0L);

        private final Sinks.Many<TxOffsetsState> offsetsStates =
            Sinks.many().replay().latestOrDefault(TxOffsetsState.INACTIVE);

        private final Sinks.Many<AcknowledgedOffset> acknowledgedOffsets =
            Sinks.unsafe().many().unicast().onBackpressureError();

        private final SerialQueue<AcknowledgedOffset> acknowledgedOffsetsQueue =
            SerialQueue.onEmitNext(acknowledgedOffsets, new ShouldBeTerminatedEmitFailureHandler(LOGGER));

        private ConsumerGroupMetadata groupMetadata = null;

        public TransactionalPoller(
            KafkaTxManager txManager,
            AssignmentSpec assignmentSpec,
            Subscriber<? super KafkaReceiverRecord<K, V>> subscriber
        ) {
            super(assignmentSpec, subscriber);
            this.txManager = txManager;
            this.transactionalOffsetsSend = acknowledgedOffsets.asFlux()
                .windowWhen(offsetsState(TxOffsetsState.PROCESSING), __ -> offsetsState(TxOffsetsState.COMMITTING))
                .concatMap(it ->
                    it.collectMap(AcknowledgedOffset::topicPartition, AcknowledgedOffset::nextOffsetAndMetadata))
                .subscribe(this::maybeSendOffsetsInCurrentTransaction, this::failSafely);
        }

        @Override
        protected void onPartitionActivated(Consumer<?, ?> consumer, ActivePartition partition) {
            groupMetadata = consumer.groupMetadata();
            partition.acknowledgedOffsets().subscribe(acknowledgedOffsetsQueue::addAndDrain, this::failSafely);
        }

        @Override
        protected void onActivePartitionsRevoked(Consumer<?, ?> consumer, List<ActivePartition> partitions) {
            groupMetadata = consumer.groupMetadata();
            Mono<TxOffsetsState> txClosure = offsetsState(TxOffsetsState.INACTIVE).next().cache();
            partitions.stream()
                .map(it -> it.deactivateTimeout(options.revocationGracePeriod(), auxiliaryScheduler))
                .collect(Collectors.collectingAndThen(Collectors.toList(), PollingSubscriptionFactory::mergeGreedily))
                .onErrorMap(TimeoutException.class, __ -> new TimeoutException("Timed out on revocation deactivation"))
                .takeUntilOther(txClosure)
                .then(txClosure.timeout(options.closeTimeout(), auxiliaryScheduler))
                .block();
        }

        @Override
        protected void onActivePartitionsLost(Map<TopicPartition, Long> lostPartitionRecordCounts) {
            // Losing partitions during transactional reception almost certainly indicates some
            // form of systemic processing degradation. However, it is conceivable that partitions
            // might be signalled as "lost" without having any participation in a recent or ongoing
            // transaction, in which case losing them isn't necessarily fatal, and we can continue,
            // letting transaction(s) possibly fail asynchronously due to stale group metadata.
            if (lostPartitionRecordCounts.values().stream().anyMatch(it -> it > 0)) {
                failSafely(new IllegalStateException("Partitions lost during transactional reception"));
            } else {
                LOGGER.warn("Partitions lost during transactional reception");
            }
        }

        @Override
        protected long prepareForActiveEmit() {
            long nvCapState = capState.get();
            if (nvCapState > 0) {
                return Math.min(super.prepareForActiveEmit(), nvCapState);
            } else if (nvCapState == 0) {
                if (activeInTransaction.get() == 0) {
                    emitOffsetsState(TxOffsetsState.COMMITTING);
                }
                return 0L;
            } else if (nvCapState == TxCapStates.COMMITTABLE) {
                maybeCommitCurrentTransaction();
                return 0L;
            } else if (nvCapState == TxCapStates.UNOPENED) {
                if (super.prepareForActiveEmit() > 0) {
                    maybeOpenNewTransaction();
                }
                return 0L;
            } else {
                // Note that it is impossible for current state to be ABORTABLE here, since the
                // only way to reach that state is due to termination invocation (which is invoked
                // from drain loop). Therefore, we don't need to worry about invoking transaction
                // abortion from here.
                return 0L;
            }
        }

        @Override
        protected boolean mayContinueActiveEmit() {
            return super.mayContinueActiveEmit() && capState.get() > 0;
        }

        @Override
        protected void handleRecordActivated(TopicPartition topicPartition) {
            activeInTransaction.incrementAndGet();
            super.handleRecordActivated(topicPartition);

            if (capState.getAndUpdate(it -> Math.max(0, it - 1)) == 1) {
                // When the capState reaches zero, we need to decrement our count of active
                // "entities" in the transaction, since the transaction itself is included in that
                // count. This completes transition of transaction state from ACTIVE to PENDING.
                activeInTransaction.decrementAndGet();
            }
        }

        @Override
        protected boolean handleRecordsDeactivated(TopicPartition topicPartition, long count) {
            long nowActive = activeInTransaction.addAndGet(-count);
            boolean drained = super.handleRecordsDeactivated(topicPartition, count);
            if (!drained && nowActive == 0) {
                drain();
                return true;
            } else {
                return drained;
            }
        }

        @Override
        protected void terminate() {
            transactionalOffsetsSend.dispose();

            long previousState =
                capState.getAndUpdate(it -> it > TxCapStates.UNOPENED ? TxCapStates.ABORTABLE : TxCapStates.TERMINATED);
            if (previousState > TxCapStates.UNOPENED) {
                maybeAbortCurrentTransaction();
            } else {
                completeTermination();
            }
        }

        private void maybeOpenNewTransaction() {
            if (capState.compareAndSet(TxCapStates.UNOPENED, TxCapStates.OPENING)) {
                transact(KafkaTxManager::begin, () -> {
                    // We initialize the count of active "entities" in our transaction to 1, in
                    // order to include the transaction itself in the count. Then, when either the
                    // batch size is reached or the period elapses, that count is removed, which
                    // then allows for it to hit zero, signalling commit eligibility.
                    activeInTransaction.set(1);
                    emitOffsetsState(TxOffsetsState.PROCESSING);

                    if (capState.compareAndSet(TxCapStates.OPENING, options.commitBatchSize())) {
                        Mono.delay(options.commitPeriod(), auxiliaryScheduler)
                            .takeUntilOther(offsetsStates.asFlux().filter(it -> it != TxOffsetsState.PROCESSING))
                            .filter(__ -> capState.getAndUpdate(it -> Math.min(0, it)) > 0)
                            .doOnNext(__ -> activeInTransaction.decrementAndGet())
                            .subscribe(__ -> drain());

                        drain();
                    }
                });
            }
        }

        private void maybeSendOffsetsInCurrentTransaction(Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (capState.compareAndSet(0, TxCapStates.OFFSETTING)) {
                if (!options.commitlessOffsets()) {
                    transact(it -> it.sendOffsets(offsets, groupMetadata), () -> {
                        if (capState.compareAndSet(TxCapStates.OFFSETTING, TxCapStates.COMMITTABLE)) {
                            drain();
                        }
                    });
                } else if (capState.compareAndSet(TxCapStates.OFFSETTING, TxCapStates.COMMITTABLE)) {
                    drain(); // Use drain over direct commit to ensure serial offsets state
                }
            }
        }

        private void maybeCommitCurrentTransaction() {
            if (capState.compareAndSet(TxCapStates.COMMITTABLE, TxCapStates.COMMITTING)) {
                transact(KafkaTxManager::commit, () -> {
                    emitOffsetsState(TxOffsetsState.INACTIVE);
                    if (capState.compareAndSet(TxCapStates.COMMITTING, TxCapStates.UNOPENED)) {
                        drain();
                    }
                });
            }
        }

        private void maybeAbortCurrentTransaction() {
            if (capState.compareAndSet(TxCapStates.ABORTABLE, TxCapStates.ABORTING)) {
                txManager.abort().doOnTerminate(this::completeTermination).subscribe(__ -> {}, this::failSafely);
            }
        }

        private void transact(Function<KafkaTxManager, Mono<Void>> invocation, Runnable onSuccess) {
            invocation.apply(txManager).subscribe(__ -> {}, this::failSafely, onSuccess);
        }

        private void completeTermination() {
            emitOffsetsState(TxOffsetsState.INACTIVE);
            capState.set(TxCapStates.TERMINATED);
        }

        private void emitOffsetsState(TxOffsetsState state) {
            try {
                offsetsStates.emitNext(state, Sinks.EmitFailureHandler.FAIL_FAST);
            } catch (Sinks.EmissionException e) {
                // This really shouldn't happen unless something has gone wrong with our state
                // management. This method is only callable from either the drain loop or the
                // transaction thread. Either has a guarantee of serial execution, and our state
                // transition predicates should ensure mutual exclusion of emission access. Still,
                // cover our bases and let ourselves know if such failure occurs.
                failSafely(e);
            }
        }

        private Flux<TxOffsetsState> offsetsState(TxOffsetsState state) {
            return offsetsStates.asFlux().filter(it -> it == state);
        }
    }

    private static final class TxCapStates {

        // Transaction usage has been aborted and/or deactivated for future use
        public static final long TERMINATED = Long.MIN_VALUE;

        // Transaction abortion has been invoked and not yet completed
        public static final long ABORTING = Long.MIN_VALUE + 1;

        // Transaction abortion has been requested and awaiting execution
        public static final long ABORTABLE = Long.MIN_VALUE + 2;

        // Transaction is inactive and eligible to be opened
        public static final long UNOPENED = Long.MIN_VALUE + 3;

        // Transaction commitment has been invoked and not yet completed
        public static final long COMMITTING = Long.MIN_VALUE + 4;

        // Transaction commitment has been requested and awaiting execution
        public static final long COMMITTABLE = Long.MIN_VALUE + 5;

        // Transaction offset sending has been requested and awaiting execution
        public static final long OFFSETTING = Long.MIN_VALUE + 6;

        // A new transaction has been requested for opening and awaiting completion
        public static final long OPENING = Long.MIN_VALUE + 7;

        private TxCapStates() {

        }
    }

    private enum TxOffsetsState {INACTIVE, PROCESSING, COMMITTING}
}
