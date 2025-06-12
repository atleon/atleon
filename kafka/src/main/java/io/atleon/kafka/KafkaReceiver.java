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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A reactive receiver of Kafka {@link ConsumerRecord}.
 *
 * @param <K>
 * @param <V>
 */
public final class KafkaReceiver<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

    private final KafkaReceiverOptions<K, V> options;

    private KafkaReceiver(KafkaReceiverOptions<K, V> options) {
        this.options = options;
    }

    public static <K, V> KafkaReceiver<K, V> create(KafkaReceiverOptions<K, V> options) {
        return new KafkaReceiver<>(options);
    }

    public Flux<KafkaReceiverRecord<K, V>> receiveManual(Collection<String> topics) {
        return receiveManual((consumer, rebalanceListener) -> consumer.subscribe(topics, rebalanceListener));
    }

    public Flux<KafkaReceiverRecord<K, V>> receiveManual(Pattern topicsPattern) {
        return receiveManual((consumer, rebalanceListener) -> consumer.subscribe(topicsPattern));
    }

    public Flux<KafkaReceiverRecord<K, V>> receiveManualWithAssignment(Collection<TopicPartition> topicPartitions) {
        return receiveManual((consumer, rebalanceListener) -> {
            consumer.assign(topicPartitions);
            rebalanceListener.onPartitionsAssigned(topicPartitions);
        });
    }

    @SuppressWarnings("ReactiveStreamsPublisherImplementation")
    private Flux<KafkaReceiverRecord<K, V>> receiveManual(AssignmentSpec assignmentSpec) {
        return Flux.from(subscriber -> subscriber.onSubscribe(new Poller(assignmentSpec, subscriber)));
    }

    private static boolean isRetriableCommitFailure(Exception exception) {
        return exception instanceof RetriableCommitFailedException
            || exception instanceof RebalanceInProgressException;
    }

    private static <T> Flux<T> mergeGreedily(Collection<? extends Publisher<? extends T>> sources) {
        return Flux.merge(Flux.fromIterable(sources), sources.size());
    }

    private static void safelyRun(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    /**
     * Sequence counter(s) for operations on partitions that have been assigned at some point
     * during reception.
     */
    private static final class SequenceSet {

        // Monotonically increasing assignment sequence numbers. Newer sequence numbers invalidate
        // committable offsets with older sequence numbers.
        private final Map<TopicPartition, AtomicLong> assignments = new ConcurrentHashMap<>();

        // Monotonically increasing commit sequence numbers. Newer sequence numbers invalidate
        // offsets whose commitment is in-flight with older sequence numbers.
        private final Map<TopicPartition, AtomicLong> commits = new ConcurrentHashMap<>();

        // Consecutive commit retry sequence numbers.
        private final Map<TopicPartition, AtomicInteger> commitRetries = new ConcurrentHashMap<>();

        public long assigned(TopicPartition partition) {
            AtomicLong assignment = assignments.computeIfAbsent(partition, __ -> new AtomicLong(0));
            commits.computeIfAbsent(partition, __ -> new AtomicLong(0));
            commitRetries.computeIfAbsent(partition, __ -> new AtomicInteger(0));
            return assignment.incrementAndGet();
        }

        public void unassigned(TopicPartition partition) {
            // For a partition that is no longer assigned, increment its assignment sequence such
            // that emitted offsets for which commitment has not yet been attempted are
            // invalidated, increment its commit sequence such that any offsets for which
            // commitment is currently being attempted/retried are invalidated, and reset its
            // commit retry count. Keep the entries for this partition around, though, in case it
            // is quickly reassigned, which could theoretically happen while there is still an
            // in-flight commit from previous assignment. Note that this method is only called when
            // it is known that the provided partition was previously assigned (so no null-check is
            // necessary).
            assignments.get(partition).incrementAndGet();
            incrementAndGetCommit(partition);
            resetCommitRetry(partition);
        }

        public long getAssignment(TopicPartition partition) {
            return assignments.get(partition).get();
        }

        public long incrementAndGetCommit(TopicPartition partition) {
            return commits.get(partition).incrementAndGet();
        }

        public long getCommit(TopicPartition partition) {
            return commits.get(partition).get();
        }

        public int getCommitRetry(TopicPartition partition) {
            return commitRetries.get(partition).get();
        }

        public void incrementRetry(TopicPartition partition) {
            commitRetries.get(partition).incrementAndGet();
        }

        public void resetCommitRetry(TopicPartition partition) {
            commitRetries.get(partition).set(0);
        }
    }

    /**
     * A partition that is currently assigned, being actively consumed, and associated with records
     * that are being processed.
     */
    private static final class ActivePartition {

        private final TopicPartition topicPartition;

        private final long sequence;

        private final Duration gracePeriod;

        // Initialized to 1, in reserve for deactivation
        private final AtomicLong registered = new AtomicLong(1);

        private final Sinks.Many<SequencedOffset> nextOffsetsOfAcknowledged =
            Sinks.unsafe().many().unicast().onBackpressureError();

        private final Flux<CommittableOffset> committableOffsets = nextOffsetsOfAcknowledged.asFlux()
            .scan((offset1, offset2) -> offset1.rawOffset() > offset2.rawOffset() ? offset1 : offset2)
            .distinctUntilChanged(SequencedOffset::rawOffset)
            .map(it -> new CommittableOffset(topicPartition(), it))
            .cache(1);

        private final SerialQueue<java.util.function.Consumer<Sinks.Many<SequencedOffset>>>
            nextOffsetsOfAcknowledgedEmissionQueue = SerialQueue.on(nextOffsetsOfAcknowledged);

        public ActivePartition(TopicPartition topicPartition, long sequence, Duration gracePeriod) {
            this.topicPartition = topicPartition;
            this.sequence = sequence;
            this.gracePeriod = gracePeriod.abs();
        }

        public <K, V> Optional<KafkaReceiverRecord<K, V>> registerForProcessing(ConsumerRecord<K, V> consumerRecord) {
            return register(new OffsetAndMetadata(consumerRecord.offset() + 1, consumerRecord.leaderEpoch(), ""))
                .map(it -> KafkaReceiverRecord.create(topicPartition, consumerRecord, it));
        }

        public Mono<CommittableOffset> deactivateAndAwaitLastCommittableOffset(Scheduler scheduler, Mono<?> disposed) {
            Mono<Void> deactivated = gracePeriod.isZero()
                ? Mono.fromRunnable(this::deactivateWithForce)
                : deactivateWithGrace(scheduler, disposed);
            return deactivated.then(committableOffsets.takeLast(1).next());
        }

        public Mono<TopicPartition> deactivateNowAndAwait() {
            deactivateWithForce();
            return committableOffsets.then(Mono.just(topicPartition));
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public Flux<CommittableOffset> committableOffsets() {
            return committableOffsets;
        }

        private Optional<Runnable> register(OffsetAndMetadata nextOffset) {
            if (gracePeriod.isZero() && registered.get() > 0) {
                // Processing has not been deactivated (forcefully or otherwise), and there is no
                // deactivation grace period, so registration is irrelevant.
                AtomicBoolean once = new AtomicBoolean(false);
                return Optional.of(() -> {
                    if (once.compareAndSet(false, true)) {
                        emitNextOffsetDueToAcknowledgement(nextOffset);
                    }
                });
            } else if (register()) {
                // Registration was successful (neither has a possible deactivation not finished,
                // nor has processing been forcibly deactivated), so allow processing attempt.
                AtomicBoolean once = new AtomicBoolean(false);
                return Optional.of(() -> {
                    if (once.compareAndSet(false, true)) {
                        emitNextOffsetDueToAcknowledgement(nextOffset);
                        deregister();
                    }
                });
            } else {
                // Registration has failed, so do not return anything for acknowledgement, which
                // will prevent emission for processing.
                return Optional.empty();
            }
        }

        private Mono<Void> deactivateWithGrace(Scheduler scheduler, Mono<?> disposed) {
            deregister();
            return committableOffsets.then()
                .timeout(gracePeriod, scheduler)
                .timeout(disposed)
                .onErrorResume(TimeoutException.class, __ -> Mono.fromRunnable(this::deactivateWithForce));
        }

        private void deactivateWithForce() {
            if (registered.getAndUpdate(parties -> parties > 0 ? Long.MIN_VALUE : parties) > 0) {
                emitCompletionOfNextOffsets();
            }
        }

        private boolean register() {
            return registered.updateAndGet(parties -> parties > 0 ? parties + 1 : parties) > 0;
        }

        private void deregister() {
            if (registered.updateAndGet(parties -> parties > 0 ? parties - 1 : parties) == 0) {
                emitCompletionOfNextOffsets();
            }
        }

        private void emitNextOffsetDueToAcknowledgement(OffsetAndMetadata nextOffset) {
            SequencedOffset nextSequencedOffset = new SequencedOffset(nextOffset, sequence);
            nextOffsetsOfAcknowledgedEmissionQueue.addAndDrain(sink -> sink.tryEmitNext(nextSequencedOffset));
        }

        private void emitCompletionOfNextOffsets() {
            nextOffsetsOfAcknowledgedEmissionQueue.addAndDrain(Sinks.Many::tryEmitComplete);
        }
    }

    private static final class EmittableRecord<K, V> {

        private final ActivePartition activePartition;

        private final ConsumerRecord<K, V> consumerRecord;

        public EmittableRecord(ActivePartition activePartition, ConsumerRecord<K, V> consumerRecord) {
            this.activePartition = activePartition;
            this.consumerRecord = consumerRecord;
        }

        public Optional<KafkaReceiverRecord<K, V>> registerForProcessing() {
            return activePartition.registerForProcessing(consumerRecord);
        }
    }

    private final class Poller implements Subscription, ReceivingConsumer.PartitioningListener {

        private final Subscriber<? super KafkaReceiverRecord<K, V>> subscriber;

        private final ReceivingConsumer<K, V> receivingConsumer;

        private final Scheduler auxiliaryScheduler;

        private final Disposable periodicCommit;

        private final int maxPollRecords = options.loadMaxPollRecords();

        private final AtomicInteger freeCapacity = new AtomicInteger(options.calculateMaxRecordsPrefetch());

        private final AtomicLong requestOutstanding = new AtomicLong(0);

        private final AtomicInteger drainsInProgress = new AtomicInteger(0);

        private final AtomicBoolean paused = new AtomicBoolean(false);

        private final AtomicBoolean done = new AtomicBoolean(false);

        private final SequenceSet sequenceSet = new SequenceSet();

        private final Map<TopicPartition, ActivePartition> assignments = new ConcurrentHashMap<>();

        private final Set<TopicPartition> externallyPausedPartitions = new CopyOnWriteArraySet<>();

        private final Queue<EmittableRecord<K, V>> emittableRecords = new ConcurrentLinkedQueue<>();

        private final Sinks.Empty<Void> disposal = Sinks.empty();

        private final Sinks.Many<CommittableOffset> committableOffsets =
            Sinks.unsafe().many().unicast().onBackpressureError();

        private final SerialQueue<CommittableOffset> committableOffsetQueue =
            SerialQueue.onEmitNext(committableOffsets);

        public Poller(AssignmentSpec assignmentSpec, Subscriber<? super KafkaReceiverRecord<K, V>> subscriber) {
            this.subscriber = subscriber;
            this.receivingConsumer = new ReceivingConsumer<>(options, this, this::doError);
            this.auxiliaryScheduler = KafkaSchedulers.newSingleForReception("auxiliary", options.loadClientId());
            this.periodicCommit = committableOffsets.asFlux()
                .windowTimeout(options.commitBatchSize(), options.commitInterval(), auxiliaryScheduler)
                .concatMap(it -> it.collectMap(CommittableOffset::topicPartition, CommittableOffset::sequencedOffset))
                .subscribe(this::scheduleCommit, this::doError);

            receivingConsumer.subscribe(assignmentSpec, this::pollAndDrain);
        }

        @Override
        public void request(long n) {
            requestOutstanding.addAndGet(n);
            drain();
        }

        @Override
        public void cancel() {
            dispose(() -> LOGGER.debug("Canceled"));
        }

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            partitions.forEach(partition -> {
                if (assignments.containsKey(partition)) {
                    throw new IllegalStateException("TopicPartition already assigned: " + partition);
                }

                long sequence = sequenceSet.assigned(partition);
                Duration deactivationGracePeriod = options.deactivationGracePeriod();
                ActivePartition activePartition = new ActivePartition(partition, sequence, deactivationGracePeriod);
                activePartition.committableOffsets().subscribe(committableOffsetQueue::addAndDrain, this::doError);
                assignments.put(partition, activePartition);
            });

            Collection<TopicPartition> partitionsToPause = calculatePartitionsToPause(partitions);
            if (!partitionsToPause.isEmpty()) {
                consumer.pause(partitionsToPause);
            }
        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            // Get the latest committable offsets for the partitions that have been revoked (with
            // possible grace period), such that we can commit them synchronously. Although it is
            // possible that these offsets may have been previously committed, it is unlikely that
            // this redundancy is relatively significant. Under low load, the overhead of a
            // synchronous commit is not likely to meaningfully degrade already-low throughput. As
            // load increases, so does the likelihood that there will be acknowledged uncommitted
            // offsets, and it becomes more desirable to attempt to honor that progress.
            List<ActivePartition> revokedPartitions = partitions.stream()
                .map(assignments::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = revokedPartitions.stream()
                .map(it -> it.deactivateAndAwaitLastCommittableOffset(auxiliaryScheduler, disposal.asMono()))
                .collect(Collectors.collectingAndThen(Collectors.toList(), KafkaReceiver::mergeGreedily))
                .collectMap(CommittableOffset::topicPartition, CommittableOffset::offsetAndMetadata)
                .block();

            try {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            } catch (WakeupException e) {
                // Corner case - There are only three plausible causes for a wakeup here:
                // 1. Emission of records freed up capacity for subsequent polling
                // 2. Reception canceled before or while awaiting completion of revoked partitions
                // 3. Async commit(s) failed and retry was exhausted
                // Every other theoretically possible wakeup must have come from faults in our code
                // (errors in process subscriptions that we aren't handling, and likely should) or
                // faulty Subscriber::onNext code (which should not throw, but rather cancel and
                // emit). To gracefully handle the plausible wakeup causes, try committing one more
                // time, and then re-emit the wakeup signal so the poll invocation knows about it.
                consumer.commitSync(offsetsToCommit);
                throw e;
            } finally {
                // Lastly, sanitize sequence counters to account for possible in-flight scheduled
                // commits and potential future reassignment.
                revokedPartitions.forEach(it -> sequenceSet.unassigned(it.topicPartition()));
            }
        }

        @Override
        public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            // No longer assigned, so impossible to commit, and all we can do is clean up.
            partitions.stream()
                .map(it -> Mono.justOrEmpty(assignments.remove(it)).flatMap(ActivePartition::deactivateNowAndAwait))
                .collect(Collectors.collectingAndThen(Collectors.toList(), KafkaReceiver::mergeGreedily))
                .doOnNext(sequenceSet::unassigned)
                .then()
                .block();
        }

        @Override
        public void onPartitionsExternallyPaused(Collection<TopicPartition> partitions) {
            externallyPausedPartitions.addAll(partitions);
        }

        @Override
        public void onPartitionsExternallyResumed(Collection<TopicPartition> partitions) {
            externallyPausedPartitions.removeAll(partitions);
        }

        private void pollAndDrain(Consumer<K, V> consumer) {
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
                if (freeCapacity.addAndGet(-queuedForEmission) < maxPollRecords) {
                    consumer.pause(assignments.keySet());
                    paused.set(true);
                    LOGGER.debug("Assignments paused");
                }
            }

            if (!done.get()) {
                receivingConsumer.schedule(this::pollAndDrain);
            }
        }

        private ConsumerRecords<K, V> pollWakeably(Consumer<K, V> consumer) {
            try {
                if (paused.get() && freeCapacity.get() >= maxPollRecords) {
                    consumer.resume(resumeablePartitions());
                    paused.set(false);
                    LOGGER.debug("Assignments resumed");
                }

                return consumer.poll(options.pollTimeout());
            } catch (WakeupException wakeup) {
                LOGGER.debug("Consumer polling woken");
                // Check if this wakeup must have been caused by freeing up capacity, and if so,
                // retry the poll.
                return paused.get() && !done.get() && freeCapacity.get() >= maxPollRecords
                    ? pollWakeably(consumer)
                    : ConsumerRecords.empty();
            }
        }

        private Collection<TopicPartition> calculatePartitionsToPause(Collection<TopicPartition> newlyAssigned) {
            if (paused.get()) {
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

        private void drain() {
            if (drainsInProgress.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            do {
                long request = requestOutstanding.get();
                long emitted = 0;
                EmittableRecord<K, V> emittableRecord;
                while (emitted < request && !done.get() && (emittableRecord = emittableRecords.poll()) != null) {
                    Optional<KafkaReceiverRecord<K, V>> registered = emittableRecord.registerForProcessing();
                    try {
                        registered.ifPresent(subscriber::onNext);
                    } catch (Throwable error) {
                        LOGGER.error("Subscriber failed onNext emission (§2.13)", error);
                        doError(error);
                    }

                    if (registered.isPresent()) {
                        emitted++;
                    }
                    // Doing this after emission, since it's possible that emission may have caused
                    // cancellation, so we may avoid unnecessary wakeup.
                    if (freeCapacity.incrementAndGet() == maxPollRecords && paused.get() && !done.get()) {
                        receivingConsumer.safelyWakeup();
                    }
                }
                requestOutstanding.addAndGet(-emitted);

                missed = drainsInProgress.addAndGet(-missed);
            } while (missed != 0);
        }

        private void scheduleCommit(Map<TopicPartition, SequencedOffset> assignedOffsets) {
            // Calculate commit sequence numbers eagerly such that invalidation may happen quickly
            // in the case that commits are being rapidly scheduled.
            Map<TopicPartition, Long> commitSequences = assignedOffsets.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), sequenceSet::incrementAndGetCommit));
            receivingConsumer.schedule(consumer -> {
                // Only need to check assignment sequence number on initial commit scheduling,
                // because we are now executing on the polling thread, and therefore any assignment
                // changes are guaranteed to visibly increase associated commit sequence
                // counter(s), so we can rely on that for invalidation (i.e. on retries).
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
            if (!validatedOffsets.isEmpty()) {
                consumer.commitAsync(validatedOffsets, (offsets, exception) -> {
                    if (manageCommittedOrShouldRetry(offsets, exception)) {
                        scheduleCommitRetry(validatedOffsets, commitSequences);
                    }
                });
            }
        }

        private boolean manageCommittedOrShouldRetry(Map<TopicPartition, OffsetAndMetadata> offsets, Exception error) {
            if (error == null) {
                offsets.keySet().forEach(sequenceSet::resetCommitRetry);
                return false;
            } else if (!isRetriableCommitFailure(error)) {
                doError(error);
                return false;
            }

            int maxRetryCount = offsets.keySet().stream()
                .mapToInt(sequenceSet::getCommitRetry)
                .reduce(0, Math::max);
            int remainingAttempts = options.maxCommitAttempts() - maxRetryCount - 1;

            if (remainingAttempts == 0) {
                doError(new KafkaException("Retries exhausted", error));
                return false;
            } else {
                LOGGER.warn("Retrying failed commit (remaining: {}): {}", remainingAttempts, error.toString());
                return true;
            }
        }

        private void scheduleCommitRetry(
            Map<TopicPartition, OffsetAndMetadata> offsets,
            Map<TopicPartition, Long> commitSequences
        ) {
            receivingConsumer.schedule(consumer -> {
                Map<TopicPartition, OffsetAndMetadata> validatedOffsets = offsets.entrySet().stream()
                    .filter(it -> commitSequences.get(it.getKey()) == sequenceSet.getCommit(it.getKey()))
                    .peek(it -> sequenceSet.incrementRetry(it.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                commit(consumer, validatedOffsets, commitSequences);
            });
        }

        private void doError(Throwable error) {
            dispose(() -> safelyRun(() -> subscriber.onError(error), "subscriber::onError §2.13"));
        }

        private void dispose(Runnable onDisposed) {
            if (done.compareAndSet(false, true)) {
                safelyRun(disposal::tryEmitEmpty, "disposal::tryEmitEmpty");
                receivingConsumer.safelyWakeup();
                receivingConsumer.safelyClose(options.closeTimeout())
                    .doOnTerminate(() -> safelyRun(periodicCommit::dispose, "periodicCommit::dispose"))
                    .doOnTerminate(() -> safelyRun(auxiliaryScheduler::dispose, "periodicScheduler::dispose"))
                    .doOnTerminate(onDisposed)
                    .subscribe();
            }
        }
    }
}
