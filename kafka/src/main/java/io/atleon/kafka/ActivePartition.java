package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueue;
import io.atleon.core.AcknowledgementQueueMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/**
 * A partition that is currently assigned, being actively consumed, and may be associated with
 * records that are being processed.
 */
final class ActivePartition {

    private final TopicPartition topicPartition;

    private final AcknowledgementQueue acknowledgementQueue;

    // This counter doubles as both our publishing state (via polarity: positive == ACTIVE,
    // negative == TERMINABLE, zero == TERMINATED) and our count of activated in-flight records
    // (via magnitude: subtract 1 if positive, negate if negative). The extra/initializing count of
    // 1 is in reserve for deactivation of this partition (self). As such, when this becomes
    // non-positive, it means this partition has been deactivated. When it becomes zero, it means
    // termination has been enqueued to subscribers of this partition's sinks.
    private final AtomicLong activated = new AtomicLong(1);

    private final Queue<Deactivation> deactivationQueue = new ConcurrentLinkedQueue<>();

    private final AtomicInteger deactivationDrainsInProgress = new AtomicInteger();

    private final Sinks.Many<OffsetAndMetadata> nextOffsetsOfAcknowledged = Sinks.unsafe().many().replay().latest();

    private final Sinks.Many<Long> deactivatedRecordCounts = Sinks.unsafe().many().unicast().onBackpressureError();

    public ActivePartition(TopicPartition topicPartition, AcknowledgementQueueMode acknowledgementQueueMode) {
        this.topicPartition = topicPartition;
        this.acknowledgementQueue = AcknowledgementQueue.create(acknowledgementQueueMode);
    }

    /**
     * Activates a {@link ConsumerRecord} for processing, which is a prerequisite for emission.
     * This will only return a non-empty result if this partition has not yet been deactivated.
     */
    public <K, V> Optional<KafkaReceiverRecord<K, V>> activateForProcessing(ConsumerRecord<K, V> consumerRecord) {
        return activate(new OffsetAndMetadata(consumerRecord.offset() + 1, consumerRecord.leaderEpoch(), ""))
            .map(it -> acknowledgementQueue.add(it, this::deactivateWithForce))
            .map(inFlight -> KafkaReceiverRecord.create(
                consumerRecord, () -> complete(inFlight), error -> completeExceptionally(inFlight, error)));
    }

    /**
     * Deactivates this partition with possible grace period, and returns the last acknowledged
     * offset which may be committed. If the grace period is non-positive, deactivation will be
     * signaled as soon as possible. A short-circuiting disposal signal may be provided such as
     * to manually timeout, which is useful if termination is requested from downstream.
     */
    public Mono<AcknowledgedOffset> deactivate(Duration gracePeriod, Scheduler scheduler, Mono<?> disposed) {
        Mono<Void> deactivated = gracePeriod.isZero() || gracePeriod.isNegative()
            ? Mono.fromRunnable(this::deactivateWithForce)
            : deactivateWithGrace(gracePeriod, scheduler, disposed);
        return deactivated.then(acknowledgedOffsets().onErrorComplete().takeLast(1).next());
    }

    /**
     * Deactivates this partition as soon as possible, and returns the {@link TopicPartition}
     * associated with this partition.
     */
    public Mono<TopicPartition> deactivateWithoutGrace() {
        deactivateWithForce();
        return nextOffsetsOfAcknowledged.asFlux().onErrorComplete().then(Mono.just(topicPartition));
    }

    /**
     * Returns a publisher of offsets that have been acknowledged and may be directly committed.
     */
    public Flux<AcknowledgedOffset> acknowledgedOffsets() {
        return nextOffsetsOfAcknowledged.asFlux().map(it -> new AcknowledgedOffset(topicPartition, it));
    }

    public Flux<Long> deactivatedRecordCounts() {
        return deactivatedRecordCounts.asFlux();
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    private Optional<Runnable> activate(OffsetAndMetadata nextOffset) {
        // If registration is successful (neither has a possible deactivation not finished, nor has
        // processing been forcibly deactivated), allow processing attempt. Else do not return
        // anything for acknowledgement, which prevents emission for processing.
        return activated.getAndUpdate(count -> count > 0 ? count + 1 : count) > 0
            ? Optional.of(() -> nextOffsetsOfAcknowledged.tryEmitNext(nextOffset))
            : Optional.empty();
    }

    private void complete(AcknowledgementQueue.InFlight inFlight) {
        acknowledge(queue -> queue.complete(inFlight));
    }

    private void completeExceptionally(AcknowledgementQueue.InFlight inFlight, Throwable error) {
        acknowledge(queue -> queue.completeExceptionally(inFlight, error));
    }

    private void acknowledge(ToLongFunction<AcknowledgementQueue> completer) {
        enqueueAndDrain(() -> {
            long completedRecords = completer.applyAsLong(acknowledgementQueue);
            long previousActivated = activated.getAndUpdate(count -> {
                if (count > 0) {
                    // Not deactivated yet, so just subtract.
                    return count - completedRecords;
                } else if (count == 0) {
                    // Deactivated and terminated, so do nothing.
                    return count;
                } else {
                    // Deactivated but not terminated yet, so add until we get to zero.
                    return count + completedRecords;
                }
            });
            // Only if previous count was negative (indicating eligibility for termination, but not
            // yet terminated) may we emit completion termination. The magnitude of a negative
            // count should be exactly equal to the number of in-flight records. Therefore, the
            // only trigger for completion will be if the previous active count was negative and
            // equal in magnitude to the number of completed records, so we can just check polarity
            // and add the two together and see if they cancel out.
            if (previousActivated < 0 && completedRecords + previousActivated == 0) {
                nextOffsetsOfAcknowledged.tryEmitComplete();
            }
            return completedRecords;
        });
    }

    private Mono<Void> deactivateWithGrace(Duration gracePeriod, Scheduler scheduler, Mono<?> disposed) {
        deactivateWithGrace();
        return nextOffsetsOfAcknowledged.asFlux()
            .then()
            .timeout(gracePeriod, scheduler)
            .timeout(disposed)
            .doOnError(TimeoutException.class, __ -> deactivateWithForce())
            .onErrorComplete();
    }

    private void deactivateWithGrace() {
        enqueueAndDrain(() -> {
            if (activated.getAndUpdate(count -> count > 0 ? 1 - count : count) == 1) {
                // This deactivation is the one that's terminating, so emit completion.
                nextOffsetsOfAcknowledged.tryEmitComplete();
            }
            return 0L;
        });
    }

    private void deactivateWithForce(Throwable error) {
        deactivateWithForce(sink -> sink.tryEmitError(error));
    }

    private void deactivateWithForce() {
        deactivateWithForce(Sinks.Many::tryEmitComplete);
    }

    private void deactivateWithForce(java.util.function.Consumer<Sinks.Many<?>> terminationEmitter) {
        enqueueAndDrain(() -> {
            long previousActivated = activated.getAndSet(0);
            if (previousActivated != 0) {
                // This deactivation is the one that's terminating, so do it and calculate the
                // number of deactivated records based on the previous count which, if positive,
                // means this partition had not yet been deactivated, so we need to subtract one,
                // and if negative, means this partition had already been deactivated, and we need
                // to negate.
                terminationEmitter.accept(nextOffsetsOfAcknowledged);
                return previousActivated > 0 ? previousActivated - 1 : -previousActivated;
            } else {
                return 0L;
            }
        });
    }

    private void enqueueAndDrain(Deactivation deactivation) {
        deactivationQueue.add(deactivation);

        if (activated.get() != 0 && deactivationDrainsInProgress.getAndIncrement() != 0) {
            return;
        }

        int missed = 1;
        do {
            long deactivatedRecordCount = 0;
            while (activated.get() != 0 && (deactivation = deactivationQueue.poll()) != null) {
                deactivatedRecordCount += deactivation.executeAndGetDeactivatedRecordCount();
            }

            if (deactivatedRecordCount != 0) {
                deactivatedRecordCounts.tryEmitNext(deactivatedRecordCount);
            }

            if (activated.get() == 0) {
                deactivatedRecordCounts.tryEmitComplete();
            }

            missed = deactivationDrainsInProgress.addAndGet(-missed);
        } while (missed != 0);
    }

    private interface Deactivation {

        long executeAndGetDeactivatedRecordCount();
    }
}
