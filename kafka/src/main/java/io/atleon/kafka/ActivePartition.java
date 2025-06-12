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
import java.util.function.Consumer;
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
            .map(inFlight -> KafkaReceiverRecord.create(
                consumerRecord, () -> complete(inFlight), error -> completeExceptionally(inFlight, error)));
    }

    /**
     * Deactivates this partition with possible grace period, and returns the last acknowledged
     * offset which may be committed. If the grace period is non-positive, deactivation will be
     * signaled as soon as possible. A short-circuiting "forced timeout" signal may be provided
     * such as to manually timeout, which is useful if termination is requested from downstream.
     */
    public Mono<AcknowledgedOffset> deactivateLatest(Duration gracePeriod, Scheduler scheduler, Mono<?> forcedTimeout) {
        return deactivateTimeout(gracePeriod, scheduler)
            .timeout(forcedTimeout)
            .onErrorComplete(GracelessTimeoutException.class)
            .onErrorResume(TimeoutException.class, __ -> deactivateForcefully())
            .then(acknowledgedOffsets().onErrorComplete().takeLast(1).next());
    }

    /**
     * Deactivates this partition with possible grace period, returning nothing as data, but
     * possibly emitting {@link TimeoutException} if either the provided grace period elapses
     * before deactivation of in-flight records is completed. If the grace period is
     * non-positive, deactivation will be immediately forced, and a {@link TimeoutException}
     * will be emitted if there were any remaining in-flight records.
     */
    public <T> Mono<T> deactivateTimeout(Duration gracePeriod, Scheduler scheduler) {
        if (gracePeriod.isZero() || gracePeriod.isNegative()) {
            return deactivateForcefully()
                .flatMap(it -> it > 0 ? Mono.error(new GracelessTimeoutException()) : Mono.empty());
        } else {
            Mono<T> acknowledgementCompletion = nextOffsetsOfAcknowledged.asFlux()
                .onErrorComplete()
                .then(Mono.<T>empty())
                .timeout(gracePeriod, scheduler);
            return deactivateGracefully().then(acknowledgementCompletion);
        }
    }

    /**
     * Forces deactivation as soon as possible and returns the number of records that were
     * deactivated.
     */
    public Mono<Long> deactivateForcefully() {
        return Mono.create(sink -> enqueueAndDrain(() -> {
            // Ensure that the activation count is updated to reflect TERMINATED state and then
            // ensure termination of next offset emission, which may be a no-op if any in-flight
            // records have been negatively acknowledged.
            long previousActivated = activated.getAndSet(0);
            nextOffsetsOfAcknowledged.tryEmitComplete();

            // Finally, calculate the number of deactivated records based on the previous count
            // which, if positive, means this partition had not yet been deactivated, so we need
            // to subtract one, and if negative, means this partition had already been deactivated,
            // and we need to negate.
            long deactivatedRecordCount = previousActivated > 0 ? previousActivated - 1 : -previousActivated;
            sink.success(deactivatedRecordCount);
            return deactivatedRecordCount;
        }));
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

    private Optional<AcknowledgementQueue.InFlight> activate(OffsetAndMetadata nextOffset) {
        // If registration is successful (neither has a possible deactivation not finished, nor has
        // processing been forcibly deactivated), allow processing attempt. Else do not return
        // anything for acknowledgement, which prevents emission for processing.
        if (activated.getAndUpdate(it -> it > 0 ? it + 1 : it) > 0) {
            Runnable acknowledger = () -> nextOffsetsOfAcknowledged.tryEmitNext(nextOffset);
            Consumer<Throwable> nacknowledger = error -> {
                nextOffsetsOfAcknowledged.tryEmitError(error);
                deactivateForcefully().subscribe();
            };
            return Optional.of(acknowledgementQueue.add(acknowledger, nacknowledger));
        } else {
            return Optional.empty();
        }
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
                    // Terminated, so do nothing.
                    return count;
                } else {
                    // Deactivated but not terminated yet, so add until we get to zero.
                    return count + completedRecords;
                }
            });
            // It is only valid to emit termination if the updated activation count has reached
            // zero. In order for that to be the case, the previous count must have been negative
            // (indicating TERMINABLE state, but not yet TERMINATED) and its magnitude must be
            // equal to the number of completed in-flight records. Therefore, we can determine if
            // we should emit termination by checking the polarity of the previous count and
            // whether the sum of completed records and the previous count cancel out.
            if (previousActivated < 0 && completedRecords + previousActivated == 0) {
                nextOffsetsOfAcknowledged.tryEmitComplete();
            }
            return completedRecords;
        });
    }

    private Mono<Void> deactivateGracefully() {
        return Mono.create(sink -> enqueueAndDrain(() -> {
            if (activated.getAndUpdate(it -> it > 0 ? 1 - it : it) == 1) {
                // This deactivation is eligible to trigger termination, so attempt to do so,
                // knowing that this could be a no-op if any in-flight records have been negatively
                // acknowledged.
                nextOffsetsOfAcknowledged.tryEmitComplete();
            }
            sink.success();
            return 0L;
        }));
    }

    private void enqueueAndDrain(Deactivation deactivation) {
        deactivationQueue.add(deactivation);

        if (deactivationDrainsInProgress.getAndIncrement() != 0) {
            return;
        }

        int missed = 1;
        do {
            long deactivatedRecordCount = 0;
            while ((deactivation = deactivationQueue.poll()) != null) {
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

    private static final class GracelessTimeoutException extends TimeoutException {

    }

    private interface Deactivation {

        long executeAndGetDeactivatedRecordCount();
    }
}
