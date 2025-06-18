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
 * A partition that is currently assigned, being actively consumed, and associated with records
 * that are being processed.
 */
final class ActivePartition {

    private final TopicPartition topicPartition;

    private final AcknowledgementQueue acknowledgementQueue;

    // Initialized to 1, in reserve for deactivation of self. When this becomes non-positive,
    // it means this partition has been deactivated. When it becomes zero, it means termination
    // has been signaled to subscribers of this partition's sinks.
    private final AtomicLong activated = new AtomicLong(1);

    private final Queue<Deactivation> deactivationQueue = new ConcurrentLinkedQueue<>();

    private final AtomicInteger deactivationDrainsInProgress = new AtomicInteger();

    private final Sinks.Many<OffsetAndMetadata> nextOffsetsOfAcknowledged = Sinks.unsafe().many().replay().latest();

    private final Sinks.Many<Long> deactivatedRecordCounts = Sinks.unsafe().many().unicast().onBackpressureError();

    public ActivePartition(TopicPartition topicPartition, AcknowledgementQueueMode acknowledgementQueueMode) {
        this.topicPartition = topicPartition;
        this.acknowledgementQueue = AcknowledgementQueue.create(acknowledgementQueueMode);
    }

    public <K, V> Optional<KafkaReceiverRecord<K, V>> activateForProcessing(ConsumerRecord<K, V> consumerRecord) {
        return activate(new OffsetAndMetadata(consumerRecord.offset() + 1, consumerRecord.leaderEpoch(), ""))
            .map(it -> acknowledgementQueue.add(it, this::deactivateWithError))
            .map(inFlight -> KafkaReceiverRecord.create(
                consumerRecord, () -> complete(inFlight), error -> completeExceptionally(inFlight, error)));
    }

    public Mono<AcknowledgedOffset> deactivate(Duration gracePeriod, Scheduler scheduler, Mono<?> disposed) {
        Mono<Void> deactivated = gracePeriod.isZero() || gracePeriod.isNegative()
            ? Mono.fromRunnable(this::deactivateWithForce)
            : deactivateWithGrace(gracePeriod, scheduler, disposed);
        return deactivated.then(acknowledgedOffsets().onErrorComplete().takeLast(1).next());
    }

    public Mono<TopicPartition> deactivateWithoutGrace() {
        deactivateWithForce();
        return nextOffsetsOfAcknowledged.asFlux()
            .onErrorComplete()
            .then(Mono.just(topicPartition));
    }

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
            long previousActiveCount = activated.getAndUpdate(count -> {
                if (count > 0) {
                    return count - completedRecords;
                } else if (count == 0) {
                    return count;
                } else {
                    return count + completedRecords;
                }
            });
            // If previous active count was positive, termination hadn't happened yet, so magnitude
            // is guaranteed to not be equal. Therefore, the only trigger for completion will be if
            // the previous active count was exactly the negative magnitude of completed records,
            // so we can just add the two together and see if they cancel out.
            if (completedRecords + previousActiveCount == 0) {
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
            if (activated.updateAndGet(count -> count > 0 ? 1 - count : count) == 0) {
                // This deactivation is the one that's terminating, so emit completion
                nextOffsetsOfAcknowledged.tryEmitComplete();
            }
            return 0L;
        });
    }

    private void deactivateWithError(Throwable error) {
        deactivateWithForce(sink -> sink.tryEmitError(error));
    }

    private void deactivateWithForce() {
        deactivateWithForce(Sinks.Many::tryEmitComplete);
    }

    private void deactivateWithForce(java.util.function.Consumer<Sinks.Many<?>> terminationEmitter) {
        enqueueAndDrain(() -> {
            long previousActiveCount = activated.getAndSet(0);
            if (previousActiveCount != 0) {
                // This deactivation is the one that's terminating, so do it and calculate the
                // number of deactivated records based on the previous count which, if positive,
                // means this partition had not yet been deactivated, so we need to subtract one,
                // and if negative, means this partition had already been deactivated, and we need
                // to negate.
                terminationEmitter.accept(nextOffsetsOfAcknowledged);
                return previousActiveCount > 0 ? previousActiveCount - 1 : -previousActiveCount;
            } else {
                return 0L;
            }
        });
    }

    private void enqueueAndDrain(Deactivation deactivation) {
        deactivationQueue.add(deactivation);

        if (deactivationDrainsInProgress.getAndIncrement() != 0) {
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
