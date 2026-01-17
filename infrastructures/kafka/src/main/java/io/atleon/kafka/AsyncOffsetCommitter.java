package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import io.atleon.core.ShouldBeTerminatedEmitFailureHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resource through which periodic asynchronous commitment of offsets is managed. Keeps track of
 * assignment and commit sequence numbers, which are used for in-flight offset commitment
 * invalidation. Such invalidation may occur due to rebalances or scheduling of commitments that
 * supersede previous scheduled commits that may not yet have completed. Also keeps track of
 * consecutive commit retry counts, which if/when exhausted, will trigger error emission.
 */
final class AsyncOffsetCommitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOffsetCommitter.class);

    private final int maxAttempts;

    private final ConsumerTaskScheduler consumerTaskScheduler;

    private final java.util.function.Consumer<Throwable> errorEmitter;

    // Monotonically increasing assignment sequence numbers. Newer sequence numbers invalidate
    // committable offsets with older sequence numbers.
    private final Map<TopicPartition, AtomicLong> assignments = new ConcurrentHashMap<>();

    // Monotonically increasing commit sequence numbers. Newer sequence numbers invalidate offsets
    // whose commitment is in-flight with older sequence numbers.
    private final Map<TopicPartition, AtomicLong> commits = new ConcurrentHashMap<>();

    // Consecutive commit retry sequence numbers.
    private final Map<TopicPartition, AtomicInteger> commitRetries = new ConcurrentHashMap<>();

    private final Sinks.Many<CommittableOffset> committableOffsets =
            Sinks.unsafe().many().unicast().onBackpressureError();

    // Only reason emission failure could/should happen is if/when we're terminating with
    // concurrent committable offset emission, hence provided failure handler.
    private final SerialQueue<CommittableOffset> committableOffsetQueue =
            SerialQueue.onEmitNext(committableOffsets, new ShouldBeTerminatedEmitFailureHandler(LOGGER));

    public AsyncOffsetCommitter(
            int maxAttempts,
            ConsumerTaskScheduler consumerTaskScheduler,
            java.util.function.Consumer<Throwable> errorEmitter) {
        this.maxAttempts = maxAttempts;
        this.consumerTaskScheduler = consumerTaskScheduler;
        this.errorEmitter = errorEmitter;
    }

    public Disposable schedulePeriodically(int batchSize, Duration period, Scheduler scheduler) {
        return committableOffsets
                .asFlux()
                .windowTimeout(batchSize, period, scheduler)
                .concatMap(it -> it.collectMap(CommittableOffset::topicPartition, CommittableOffset::sequencedOffset))
                .subscribe(this::scheduleCommit, errorEmitter);
    }

    public java.util.function.Consumer<AcknowledgedOffset> acknowledgementHandlerForAssigned(TopicPartition partition) {
        AtomicLong assignmentSequenceCounter = assignments.computeIfAbsent(partition, __ -> new AtomicLong(0));
        commits.computeIfAbsent(partition, __ -> new AtomicLong(0));
        commitRetries.computeIfAbsent(partition, __ -> new AtomicInteger(0));

        long assignmentSequence = assignmentSequenceCounter.incrementAndGet();
        return it -> committableOffsetQueue.addAndDrain(new CommittableOffset(it, assignmentSequence));
    }

    public void unassigned(TopicPartition partition) {
        AtomicLong assignmentSequenceCounter = assignments.get(partition);
        if (assignmentSequenceCounter != null) {
            // For a partition that is un-assigned, increment its assignment sequence such that
            // emitted offsets for which commitment has not yet been attempted are invalidated,
            // increment its commit sequence such that any offsets for which commitment is
            // currently being attempted/retried are invalidated, and reset its commit retry count.
            // Keep any entries for this partition around, though, in case it is quickly
            // reassigned, which could theoretically happen while there is still an in-flight
            // commit from previous assignment. Note that this method is always invoked from the
            // polling thread, and the Kafka client takes care of previously clearing out any
            // in-flight asynchronous commit invocations, so none should be concurrently executing
            // at this point. Also note that further null-checks are unnecessary, because if this
            // partition was ever assigned (determinable by having an assignment sequence counter),
            // then it will have all sequence counters.
            assignmentSequenceCounter.incrementAndGet();
            incrementAndGetCommit(partition);
            resetCommitRetry(partition);
        }
    }

    public boolean isCommitTrialExhausted(TopicPartition partition) {
        return calculateRemainingCommitAttempts(partition) <= 0;
    }

    private void scheduleCommit(Map<TopicPartition, SequencedOffset> assignedOffsets) {
        // Calculate commit sequence numbers eagerly such that invalidation may happen quickly in
        // the case that commits are being rapidly scheduled.
        Map<TopicPartition, Long> commitSequences = assignedOffsets.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), this::incrementAndGetCommit));
        consumerTaskScheduler.schedule(consumer -> {
            // Only need to check assignment sequence number on initial commit attempt, because we
            // are now executing on the polling thread, and therefore any assignment changes are
            // guaranteed to visibly increase associated commit sequence counter(s), so we can rely
            // on that for invalidation (i.e. on retries).
            Map<TopicPartition, OffsetAndMetadata> validatedOffsets = assignedOffsets.entrySet().stream()
                    .filter(it -> it.getValue().sequence() == getAssignment(it.getKey()))
                    .filter(it -> commitSequences.get(it.getKey()) == getCommit(it.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey, it -> it.getValue().offsetAndMetadata()));
            commit(consumer, validatedOffsets, commitSequences);
        });
    }

    private void commit(
            Consumer<?, ?> consumer,
            Map<TopicPartition, OffsetAndMetadata> validatedOffsets,
            Map<TopicPartition, Long> commitSequences) {
        if (validatedOffsets.isEmpty()) {
            return;
        }

        consumer.commitAsync(validatedOffsets, (offsets, exception) -> {
            if (exception == null) {
                offsets.keySet().forEach(this::resetCommitRetry);
                return;
            } else if (!KafkaErrors.isRetriableCommitFailure(exception)) {
                errorEmitter.accept(exception);
                return;
            }

            int remainingAttempts = offsets.keySet().stream()
                    .mapToInt(this::calculateRemainingCommitAttempts)
                    .reduce(maxAttempts, Math::min);

            if (remainingAttempts == 0) {
                errorEmitter.accept(new KafkaException("Retries exhausted", exception));
            } else {
                LOGGER.warn("Retrying failed commit (remaining: {}): {}", remainingAttempts, exception.toString());
                scheduleCommitRetry(offsets, commitSequences);
            }
        });
    }

    private void scheduleCommitRetry(
            Map<TopicPartition, OffsetAndMetadata> offsets, Map<TopicPartition, Long> commitSequences) {
        consumerTaskScheduler.schedule(consumer -> {
            Map<TopicPartition, OffsetAndMetadata> validatedOffsets = offsets.entrySet().stream()
                    .filter(it -> commitSequences.get(it.getKey()) == getCommit(it.getKey()))
                    .peek(it -> incrementCommitRetry(it.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            commit(consumer, validatedOffsets, commitSequences);
        });
    }

    private long getAssignment(TopicPartition partition) {
        return assignments.get(partition).get();
    }

    private long incrementAndGetCommit(TopicPartition partition) {
        return commits.get(partition).incrementAndGet();
    }

    private long getCommit(TopicPartition partition) {
        return commits.get(partition).get();
    }

    private int calculateRemainingCommitAttempts(TopicPartition partition) {
        AtomicInteger commitRetryCounter = commitRetries.get(partition);
        int commitRetryCount = commitRetryCounter == null ? 0 : commitRetryCounter.get();
        return maxAttempts - commitRetryCount - 1;
    }

    private void incrementCommitRetry(TopicPartition partition) {
        commitRetries.get(partition).incrementAndGet();
    }

    private void resetCommitRetry(TopicPartition partition) {
        commitRetries.get(partition).set(0);
    }

    @FunctionalInterface
    public interface ConsumerTaskScheduler {

        void schedule(java.util.function.Consumer<Consumer<?, ?>> task);
    }

    private static final class CommittableOffset {

        private final TopicPartition topicPartition;

        private final OffsetAndMetadata offsetAndMetadata;

        private final long sequence;

        public CommittableOffset(AcknowledgedOffset acknowledgedOffset, long sequence) {
            this.topicPartition = acknowledgedOffset.topicPartition();
            this.offsetAndMetadata = acknowledgedOffset.nextOffsetAndMetadata();
            this.sequence = sequence;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public SequencedOffset sequencedOffset() {
            return new SequencedOffset(offsetAndMetadata, sequence);
        }
    }

    private static final class SequencedOffset {

        private final OffsetAndMetadata offsetAndMetadata;

        private final long sequence;

        public SequencedOffset(OffsetAndMetadata offsetAndMetadata, long sequence) {
            this.offsetAndMetadata = offsetAndMetadata;
            this.sequence = sequence;
        }

        public OffsetAndMetadata offsetAndMetadata() {
            return offsetAndMetadata;
        }

        public long sequence() {
            return sequence;
        }
    }
}
