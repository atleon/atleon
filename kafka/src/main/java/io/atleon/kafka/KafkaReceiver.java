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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A reactive receiver of Kafka {@link ConsumerRecord records}, which are wrapped as
 * {@link KafkaReceiverRecord}. A new Kafka {@link Consumer} is associated with each receiver
 * subscription, and closed if/when that subscription is canceled or errored out. Each reception
 * subscription takes care of keeping track of the records that have been emitted and acknowledged
 * on a per-partition, per-assignment basis, and will only make any given record's offset available
 * for commit (which is done with a configurable periodic interval) if/when that record and all the
 * records that came before it in the same active partition assignment have been acknowledged. It
 * is therefore important that every emitted record be either positively acknowledged when its
 * processing completes normally, or negatively acknowledged ("nacknowledged"), in the case of
 * processing failure.
 * <p>
 * Upon "deactivation" of any assigned partition, whether through typical revocation from a
 * rebalance, reception error, or downstream cancellation, a strong effort is made to ensure that
 * any offsets which can be committed are done so synchronously. The only case in which this is not
 * true is if partitions are signaled to have been "lost", which can happen if/when a consumer is
 * unexpectedly removed from its group (i.e. due to session timeout). Otherwise, a grace period is
 * configurable, which configures a maximum amount of time that will be awaited for in-flight
 * records to be acknowledged, before attempting final commitment and releasing any assigned
 * partition(s). This grace period can be set to zero such that only record offsets acknowledged at
 * the immediate time of partition deactivation will be committed immediately, which makes
 * rebalancing faster at the cost of higher re-processing likelihood.
 * <p>
 * This receiver plays well with cooperative rebalancing by allowing polling and processing to
 * continue during a cooperative rebalance, and taking care of cleanup if/when such rebalancing
 * results in partition revocation.
 *
 * @param <K> The type of keys in records emitted by this receiver
 * @param <V> The type of values in records emitted by this receiver
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

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private enum State {ACTIVE, TERMINABLE, TERMINATED}

    private final class Poller implements Subscription, ReceivingConsumer.PartitionListener {

        private final Subscriber<? super KafkaReceiverRecord<K, V>> subscriber;

        private final ReceivingConsumer<K, V> receivingConsumer;

        private final ReceptionListener listener;

        private final Scheduler auxiliaryScheduler;

        private final Disposable periodicCommit;

        private final int maxPollRecords = options.loadMaxPollRecords();

        private final AtomicInteger freePrefetchCapacity = new AtomicInteger(options.calculateMaxRecordsPrefetch());

        private final AtomicLong requestOutstanding = new AtomicLong(0);

        private final AtomicInteger drainsInProgress = new AtomicInteger(0);

        private final AtomicLong freeActiveInFlightCapacity = new AtomicLong(options.maxActiveInFlight());

        private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

        private final AtomicBoolean pausedDueToBackpressure = new AtomicBoolean(false);

        private final AtomicReference<Throwable> error = new AtomicReference<>();

        private final ReceptionSequenceSet sequenceSet = new ReceptionSequenceSet();

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
            this.receivingConsumer = new ReceivingConsumer<>(options, this, this::failSafely);
            this.listener = options.createReceptionListener();
            this.auxiliaryScheduler = options.createAuxiliaryScheduler();
            this.periodicCommit = committableOffsets.asFlux()
                .windowTimeout(options.commitBatchSize(), options.commitInterval(), auxiliaryScheduler)
                .concatMap(it -> it.collectMap(CommittableOffset::topicPartition, CommittableOffset::sequencedOffset))
                .subscribe(this::scheduleCommit, this::failSafely);

            receivingConsumer.subscribe(assignmentSpec, this::pollAndDrain);
        }

        @Override
        public void request(long n) {
            if (n > 0 && requestOutstanding.get() != Long.MAX_VALUE) {
                requestOutstanding.addAndGet(n);
                drain();
            }
        }

        @Override
        public void cancel() {
            if (state.compareAndSet(State.ACTIVE, State.TERMINABLE)) {
                drain();
            }
        }

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            partitions.forEach(partition -> {
                if (assignments.containsKey(partition)) {
                    throw new IllegalStateException("TopicPartition already assigned: " + partition);
                }

                ActivePartition activePartition = new ActivePartition(partition, options.acknowledgementQueueMode());
                listener.onPartitionActivated(partition);

                long assignmentSequence = sequenceSet.assigned(partition);
                activePartition.acknowledgedOffsets()
                    .map(it -> new CommittableOffset(it, assignmentSequence))
                    .subscribe(committableOffsetQueue::addAndDrain, this::failSafely);
                activePartition.deactivatedRecordCounts()
                    .doOnNext(it -> listener.onRecordsDeactivated(partition, it))
                    .subscribe(this::handleInFlightRecordsDeactivated, this::failSafely);
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
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            // Get the latest committable offsets for the partitions that have been revoked (with
            // possible grace period), such that we can commit them synchronously. Although it is
            // possible that these offsets may have been previously committed, it is unlikely that
            // this redundancy is significantly undesirable. Under low load, the overhead of a
            // synchronous commit is not likely to meaningfully degrade already-low throughput. As
            // load increases, so does the likelihood that there will be acknowledged uncommitted
            // offsets, and it becomes more desirable to attempt to honor that progress.
            List<ActivePartition> revokedPartitions = partitions.stream()
                .map(assignments::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = revokedPartitions.stream()
                .map(it -> it.deactivate(options.revocationGracePeriod(), auxiliaryScheduler, disposal.asMono()))
                .collect(Collectors.collectingAndThen(Collectors.toList(), KafkaReceiver::mergeGreedily))
                .filter(it -> sequenceSet.getCommitRetry(it.topicPartition()) < options.maxCommitAttempts() - 1)
                .collectMap(AcknowledgedOffset::topicPartition, AcknowledgedOffset::nextOffsetAndMetadata)
                .block();

            try {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            } catch (WakeupException e) {
                // There are two possible causes for a wakeup during partition revocation:
                //   1. Async emission of records freed up capacity for subsequent polling
                //   2. Async termination (without poll invocation consuming wakeup signal)
                // In order to attempt graceful handling of either scenario, we retry the commit.
                // Although it is not likely, it is possible that both scenarios occur (in order),
                // with the first retry attempt being woken by the second scenario. In this case,
                // we retry one LAST time. We then re-emit the original wakeup signal so that any
                // possible parent poll invocation knows about it.
                try {
                    consumer.commitSync(offsetsToCommit);
                } catch (WakeupException __) {
                    consumer.commitSync(offsetsToCommit);
                }
                throw e;
            } finally {
                // Lastly, sanitize sequence counters to account for possible in-flight commits and
                // potential future reassignment (and signal listener about deactivation).
                revokedPartitions.forEach(it -> sequenceSet.unassigned(it.topicPartition()));
                revokedPartitions.forEach(it -> listener.onPartitionDeactivated(it.topicPartition()));
            }
        }

        @Override
        public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            // No longer assigned, so impossible to commit, and all we can do is clean up.
            List<TopicPartition> lostPartitions = partitions.stream()
                .map(it -> Mono.justOrEmpty(assignments.remove(it)).flatMap(ActivePartition::deactivateWithoutGrace))
                .collect(Collectors.collectingAndThen(Collectors.toList(), KafkaReceiver::mergeGreedily))
                .collectList()
                .block();

            lostPartitions.forEach(sequenceSet::unassigned);
            lostPartitions.forEach(listener::onPartitionDeactivated);
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
            if (state.get() == State.TERMINATED) {
                return;
            }

            // Reschedule this invocation before actually invoking poll, in order to ensure that
            // any wakeup from freed prefetch capacity will be seen by a poll invocation.
            receivingConsumer.schedule(this::pollAndDrain);

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

        private void handleInFlightRecordsDeactivated(long deactivationCount) {
            if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                freeActiveInFlightCapacity.addAndGet(deactivationCount);
                drain();
            }
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

        private void failSafely(Throwable failure) {
            if (state.get() != State.ACTIVE || !error.compareAndSet(null, failure)) {
                // Failures during termination and failures that don't initiate termination can be
                // safely dropped.
                LOGGER.debug("Ignoring failure during termination", failure);
            } else if (state.compareAndSet(State.ACTIVE, State.TERMINABLE)) {
                // Could be racing with cancellation, but it's not a spec violation if onError
                // emission is concurrent with downstream cancellation.
                drain();
            }
        }

        private void drain() {
            if (state.get() == State.TERMINATED || drainsInProgress.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            do {
                // Handle onNext emission and update outstanding capacities.
                long emitted = drainEmittable(Math.min(freeActiveInFlightCapacity.get(), requestOutstanding.get()));
                if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                    freeActiveInFlightCapacity.addAndGet(-emitted);
                }
                if (requestOutstanding.get() != Long.MAX_VALUE) {
                    requestOutstanding.addAndGet(-emitted);
                }

                // Handle termination, if now is the time to do so. Don't need CAS here since this
                // is the only place to transition to TERMINATED.
                if (state.get() == State.TERMINABLE) {
                    terminateSafely();
                    state.set(State.TERMINATED);
                    receivingConsumer.wakeupSafely();
                }

                missed = drainsInProgress.addAndGet(-missed);
            } while (missed != 0);
        }

        private long drainEmittable(long maxEmits) {
            long emitted = 0;
            EmittableRecord<K, V> emittable;
            while (emitted < maxEmits && state.get() == State.ACTIVE && (emittable = emittableRecords.poll()) != null) {
                Optional<KafkaReceiverRecord<K, V>> activated = emittable.activateForProcessing();
                try {
                    listener.onRecordsActivated(emittable.topicPartition(), activated.isPresent() ? 1 : 0);
                    activated.ifPresent(subscriber::onNext);
                } catch (Throwable error) {
                    LOGGER.error("Emission failure (§2.13)", error);
                    failSafely(error);
                }

                if (activated.isPresent()) {
                    emitted++;
                }

                // Doing this after emission, since it's possible that emission may have caused
                // cancellation, so we may avoid unnecessary wakeup.
                if (freePrefetchCapacity.incrementAndGet() == maxPollRecords && activelyPausedDueToBackpressure()) {
                    receivingConsumer.wakeupSafely();
                }
            }
            return emitted;
        }

        private void terminateSafely() {
            if (error.get() != null) {
                runSafely(() -> subscriber.onError(error.get()), "subscriber::onError §2.13");
                LOGGER.debug("Terminated due to error");
            } else {
                LOGGER.debug("Terminated due to cancel");
            }

            runSafely(disposal::tryEmitEmpty, "disposal::tryEmitEmpty");
            receivingConsumer.closeSafely()
                .doOnTerminate(() -> runSafely(listener::close, "listener::close"))
                .doOnTerminate(() -> runSafely(periodicCommit::dispose, "periodicCommit::dispose"))
                .doOnTerminate(() -> runSafely(auxiliaryScheduler::dispose, "periodicScheduler::dispose"))
                .subscribe();
        }

        private boolean activelyPausedDueToBackpressure() {
            return pausedDueToBackpressure.get() && state.get() == State.ACTIVE;
        }
    }
}
