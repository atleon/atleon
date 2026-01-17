package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Realizes the constraints of bounded polling by attaching to the available extension points for
 * partition assignment, polling invocation, and reception lifecycle.
 */
final class BoundedPolling implements ConsumerListener, ReceptionListener, PollStrategy {

    private final Map<TopicPartition, Long> minInclusiveOffsets;

    private final Map<TopicPartition, Long> maxInclusiveOffsets;

    private final int maxConcurrentPartitions;

    private final Set<TopicPartition> permittedPartitions = new HashSet<>();

    private final Sinks.Empty<Void> closed = Sinks.unsafe().empty();

    private final Sinks.Many<Integer> polledRecordCounts =
            Sinks.unsafe().many().unicast().onBackpressureError();

    private final Sinks.Many<Long> deactivatedRecordCounts =
            Sinks.unsafe().many().unicast().onBackpressureError();

    private final SerialQueue<Long> deactivatedRecordCountQueue = SerialQueue.onEmitNext(deactivatedRecordCounts);

    private final Flux<Long> deactivatedRecordCountTotals = deactivatedRecordCounts
            .asFlux()
            .scan(0L, Long::sum)
            .cache(1)
            .publish()
            .autoConnect(0);

    public BoundedPolling(Collection<RecordRange> recordRanges, int maxConcurrentPartitions) {
        this.minInclusiveOffsets =
                recordRanges.stream().collect(Collectors.toMap(RecordRange::topicPartition, RecordRange::minInclusive));
        this.maxInclusiveOffsets = recordRanges.stream()
                .collect(Collectors.toMap(
                        RecordRange::topicPartition, RecordRange::maxInclusive, (l, r) -> l, LinkedHashMap::new));
        this.maxConcurrentPartitions = maxConcurrentPartitions;
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Long minInclusiveOffset = minInclusiveOffsets.remove(partition);
            if (minInclusiveOffset != null) {
                consumer.seek(partition, minInclusiveOffset);
            }
        }
    }

    @Override
    public void close() {
        closed.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
    }

    @Override
    public void onRecordsDeactivated(TopicPartition partition, long count) {
        deactivatedRecordCountQueue.addAndDrain(count);
    }

    @Override
    public void onPollingPermitted(Collection<TopicPartition> partitions) {
        permittedPartitions.addAll(partitions);
    }

    @Override
    public void onPollingProhibited(Collection<TopicPartition> partitions) {
        permittedPartitions.removeAll(partitions);
    }

    @Override
    public void prepareForPoll(PollSelectionContext context) {
        Set<TopicPartition> partitions = maxInclusiveOffsets.keySet().stream()
                .filter(permittedPartitions::contains)
                .limit(maxConcurrentPartitions)
                .collect(Collectors.toSet());
        context.selectExclusively(partitions);
    }

    @Override
    public <K, V> ConsumerRecords<K, V> onPoll(ConsumerRecords<K, V> consumerRecords) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> truncatedConsumerRecords = new LinkedHashMap<>();
        for (TopicPartition partition : consumerRecords.partitions()) {
            List<ConsumerRecord<K, V>> records = consumerRecords.records(partition);

            long maxInclusiveOffset = maxInclusiveOffsets.get(partition);
            if (records.get(records.size() - 1).offset() < maxInclusiveOffset) {
                truncatedConsumerRecords.put(partition, records);
            } else {
                // Should be no harm in potentially adding empty list of records due to truncation
                truncatedConsumerRecords.put(partition, truncate(records, maxInclusiveOffset));
                maxInclusiveOffsets.remove(partition);
            }
        }

        int recordCount =
                truncatedConsumerRecords.values().stream().mapToInt(List::size).sum();
        if (recordCount != 0) {
            polledRecordCounts.emitNext(recordCount, Sinks.EmitFailureHandler.FAIL_FAST);
        }

        if (maxInclusiveOffsets.isEmpty()) {
            polledRecordCounts.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
        }

        return new ConsumerRecords<>(truncatedConsumerRecords);
    }

    public Mono<Long> pollingAndProcessingCompleted(Duration processingGracePeriod, Scheduler timer) {
        return polledRecordCounts
                .asFlux()
                .reduce(0L, Long::sum)
                .delayUntil(pollTotal -> awaitProcessingCompletion(pollTotal, processingGracePeriod, timer));
    }

    public <T> Mono<T> closed() {
        return closed.asMono().then(Mono.empty());
    }

    private Mono<Void> awaitProcessingCompletion(Long pollTotal, Duration timeout, Scheduler timer) {
        String timeoutMessage = "Processing timed out. You must acknowledge received records within timeout=" + timeout;
        return deactivatedRecordCountTotals
                .takeUntil(pollTotal::equals)
                .then()
                .timeout(timeout, timer)
                .onErrorMap(TimeoutException.class, __ -> new TimeoutException(timeoutMessage));
    }

    private static <K, V> List<ConsumerRecord<K, V>> truncate(
            List<ConsumerRecord<K, V>> records, long maxInclusiveOffset) {
        for (int i = records.size() - 1; i >= 0; i--) {
            if (records.get(i).offset() <= maxInclusiveOffset) {
                return records.subList(0, i + 1);
            }
        }
        return Collections.emptyList();
    }
}
