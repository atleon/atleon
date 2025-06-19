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
//TODO It's possible that onNext and onError may not be serialized
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

    private final class Poller implements Subscription, ReceivingConsumer.PartitionListener {

        private final Subscriber<? super KafkaReceiverRecord<K, V>> subscriber;

        private final ReceivingConsumer<K, V> receivingConsumer;

        private final ReceptionListener listener;

        private final Scheduler auxiliaryScheduler;

        private final Disposable periodicCommit;

        private final int maxPollRecords = options.loadMaxPollRecords();

        private final AtomicInteger freePrefetchCapacity = new AtomicInteger(options.calculateMaxRecordsPrefetch());

        private final AtomicLong freeActiveInFlightCapacity = new AtomicLong(options.maxActiveInFlight());

        private final AtomicLong requestOutstanding = new AtomicLong(0);

        private final AtomicInteger drainsInProgress = new AtomicInteger(0);

        private final AtomicBoolean paused = new AtomicBoolean(false);

        private final AtomicBoolean done = new AtomicBoolean(false);

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
            this.receivingConsumer = new ReceivingConsumer<>(options, this, this::doError);
            this.listener = options.createReceptionListener();
            this.auxiliaryScheduler = options.createAuxiliaryScheduler();
            this.periodicCommit = committableOffsets.asFlux()
                .windowTimeout(options.commitBatchSize(), options.commitInterval(), auxiliaryScheduler)
                .concatMap(it -> it.collectMap(CommittableOffset::topicPartition, CommittableOffset::sequencedOffset))
                .subscribe(this::scheduleCommit, this::doError);

            receivingConsumer.subscribe(assignmentSpec, this::pollAndDrain);
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                requestOutstanding.addAndGet(n);
                drain();
            }
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

                listener.onPartitionActivated(partition);

                long assignmentSequence = sequenceSet.assigned(partition);

                ActivePartition activePartition = new ActivePartition(partition, options.acknowledgementQueueMode());
                activePartition.acknowledgedOffsets()
                    .map(it -> new CommittableOffset(it, assignmentSequence))
                    .subscribe(committableOffsetQueue::addAndDrain, this::doError);
                activePartition.deactivatedRecordCounts()
                    .doOnNext(it -> listener.onRecordsDeactivated(partition, it))
                    .subscribe(this::handleInFlightRecordsDeactivated, this::doError);
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
            // this redundancy is relatively significant. Under low load, the overhead of a
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
                // Corner case - There are only three plausible causes for a wakeup here:
                // 1. Emission of records freed up capacity for subsequent polling
                // 2. Reception terminated before or while awaiting revocation completion
                // 3. Async commit(s) failed and retry was exhausted
                // Every other theoretically possible wakeup must have come from faults in our code
                // (errors in process subscriptions that we aren't handling, and almost certainly
                // should) or faulty Subscriber::onNext code (which should not throw, but rather
                // cancel and emit). In order to attempt graceful handling of the plausible wakeup
                // causes, try committing one LAST time, and then re-emit the wakeup signal so the
                // poll invocation knows about it.
                consumer.commitSync(offsetsToCommit);
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
                if (paused.get() && freePrefetchCapacity.get() >= maxPollRecords) {
                    consumer.resume(resumeablePartitions());
                    paused.set(false);
                    LOGGER.debug("Assignments resumed");
                }

                return consumer.poll(options.pollTimeout());
            } catch (WakeupException wakeup) {
                LOGGER.debug("Consumer polling woken");
                // Check if this wakeup must have been caused by freeing up capacity, and if so,
                // retry the poll.
                return paused.get() && !done.get() && freePrefetchCapacity.get() >= maxPollRecords
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

        private void handleInFlightRecordsDeactivated(long count) {
            if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                freeActiveInFlightCapacity.addAndGet(count);
                drain();
            }
        }

        private void drain() {
            if (drainsInProgress.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            do {
                long maxToEmit = Math.min(freeActiveInFlightCapacity.get(), requestOutstanding.get());
                long emitted = 0;
                EmittableRecord<K, V> emittableRecord;
                while (emitted < maxToEmit && !done.get() && (emittableRecord = emittableRecords.poll()) != null) {
                    Optional<KafkaReceiverRecord<K, V>> activated = emittableRecord.activateForProcessing();
                    try {
                        listener.onRecordsActivated(emittableRecord.topicPartition(), activated.isPresent() ? 1 : 0);
                        activated.ifPresent(subscriber::onNext);
                    } catch (Throwable error) {
                        LOGGER.error("Emission failure (§2.13)", error);
                        doError(error);
                    }

                    if (activated.isPresent()) {
                        emitted++;
                    }

                    // Doing this after emission, since it's possible that emission may have caused
                    // cancellation, so we may avoid unnecessary wakeup.
                    if (freePrefetchCapacity.incrementAndGet() == maxPollRecords && paused.get() && !done.get()) {
                        receivingConsumer.safelyWakeup();
                    }
                }

                if (freeActiveInFlightCapacity.get() != Long.MAX_VALUE) {
                    freeActiveInFlightCapacity.addAndGet(-emitted);
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
            if (validatedOffsets.isEmpty()) {
                return;
            }

            consumer.commitAsync(validatedOffsets, (offsets, exception) -> {
                if (exception == null) {
                    offsets.keySet().forEach(sequenceSet::resetCommitRetry);
                    return;
                } else if (!isRetriableCommitFailure(exception)) {
                    doError(exception);
                    return;
                }

                int maxRetryCount = offsets.keySet().stream()
                    .mapToInt(sequenceSet::getCommitRetry)
                    .reduce(0, Math::max);
                int remainingAttempts = options.maxCommitAttempts() - maxRetryCount - 1;

                if (remainingAttempts == 0) {
                    doError(new KafkaException("Retries exhausted", exception));
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
                    .doOnTerminate(() -> safelyRun(listener::close, "listener::close"))
                    .doOnTerminate(onDisposed)
                    .subscribe();
            }
        }
    }
}
