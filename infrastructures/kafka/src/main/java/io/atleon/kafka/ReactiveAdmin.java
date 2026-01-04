package io.atleon.kafka;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/**
 * Wrapper around a Kafka {@link Admin} instance providing a reactive facade for commonly used
 * utility functions.
 */
public class ReactiveAdmin implements Closeable {

    private static final ListOffsetsOptions LIST_OFFSETS_OPTIONS =
            new ListOffsetsOptions(IsolationLevel.READ_COMMITTED);

    private final Admin admin;

    ReactiveAdmin(Admin admin) {
        this.admin = admin;
    }

    public static ReactiveAdmin create(Map<String, Object> config) {
        return new ReactiveAdmin(Admin.create(config));
    }

    /**
     * Alters offsets for the specified group. In order to succeed, the group must be empty, i.e.
     * not actively consuming.
     *
     * @param groupId The group for which to alter offsets.
     * @param offsets A map of raw offsets by partition
     * @return A {@link Mono} that signals success or error of offset alteration
     */
    public Mono<Void> alterRawConsumerGroupOffsets(String groupId, Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = offsets.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, it -> new OffsetAndMetadata(it.getValue())));
        return execute(admin ->
                admin.alterConsumerGroupOffsets(groupId, offsetsAndMetadata).all());
    }

    /**
     * Describes the current state of committed offsets for the provided consumer group ID, along
     * with the latest offsets for each of the {@link TopicPartition}s for which the group has
     * committed offsets, thus allowing for an estimation of the group's lag.
     * <p>
     * Note that if a consumer is consuming or has consumed messages from any given topic's
     * partition, but not yet committed any offsets for that partition, then there will be no data
     * emitted for that {@link TopicPartition}.
     *
     * @param groupId The consumer group ID to describe offsets for
     * @return A {@link Flux} of {@link TopicPartitionGroupOffsets} describing group and partition offsets
     */
    public Flux<TopicPartitionGroupOffsets> listTopicPartitionGroupOffsets(String groupId) {
        return listTopicPartitionGroupOffsets(Collections.singletonList(groupId));
    }

    /**
     * Describes the current state of committed offsets for the provided consumer group IDs, along
     * with the latest offsets for each of the {@link TopicPartition}s for which the groups have
     * committed offsets, thus allowing for an estimation of the groups' lag.
     * <p>
     * Note that if a consumer is consuming or has consumed messages from any given topic's
     * partition, but not yet committed any offsets for that partition, then there will be no data
     * emitted for that {@link TopicPartition}.
     *
     * @param groupIds The consumer group IDs to describe offsets for
     * @return A {@link Flux} of {@link TopicPartitionGroupOffsets} describing group and partition offsets
     */
    public Flux<TopicPartitionGroupOffsets> listTopicPartitionGroupOffsets(Collection<String> groupIds) {
        Map<String, ListConsumerGroupOffsetsSpec> offsetSpecs = groupIds.stream()
                .collect(Collectors.toMap(Function.identity(), __ -> new ListConsumerGroupOffsetsSpec()));
        return execute(admin -> admin.listConsumerGroupOffsets(offsetSpecs).all())
                .flatMapMany(this::listTopicPartitionGroupOffsets);
    }

    /**
     * Describes the current state of committed offsets for the provided {@link TopicPartition}s and
     * consumer group IDs. There will be one result for each existing {@link TopicPartition} and
     * group ID. If there is not a committed offset for a given group ID, then the provided
     * {@link OffsetResetStrategy} will be used to determine where the consumer group <i>would</i>
     * begin consuming from.
     *
     * @param groupId         The consumer group ID to describe offsets for
     * @param resetStrategy   Where consumption would begin if there are no committed offsets
     * @param topicPartitions The {@link TopicPartition}s to describe offsets for
     * @return A {@link Flux} of {@link TopicPartitionGroupOffsets} describing group and partition offsets
     */
    public Flux<TopicPartitionGroupOffsets> listTopicPartitionGroupOffsets(
            String groupId, OffsetResetStrategy resetStrategy, Collection<TopicPartition> topicPartitions) {
        return listTopicPartitionGroupOffsets(Collections.singletonMap(groupId, resetStrategy), topicPartitions);
    }

    /**
     * Describes the current state of committed offsets for the provided {@link TopicPartition}s and
     * consumer group IDs. There will be one result for each existing {@link TopicPartition} and
     * group ID. If there is not a committed offset for a given group ID, then the provided
     * {@link OffsetResetStrategy} will be used to determine where the consumer group <i>would</i>
     * begin consuming from.
     *
     * @param groupIds        Group IDs to describe offsets for and where consumption would begin if none exist
     * @param topicPartitions The {@link TopicPartition}s to describe offsets for
     * @return A {@link Flux} of {@link TopicPartitionGroupOffsets} describing group and partition offsets
     */
    public Flux<TopicPartitionGroupOffsets> listTopicPartitionGroupOffsets(
            Map<String, OffsetResetStrategy> groupIds, Collection<TopicPartition> topicPartitions) {
        ListConsumerGroupOffsetsSpec offsetsSpec = new ListConsumerGroupOffsetsSpec().topicPartitions(topicPartitions);
        Map<String, ListConsumerGroupOffsetsSpec> offsetSpecs =
                groupIds.keySet().stream().collect(Collectors.toMap(Function.identity(), __ -> offsetsSpec));
        return Mono.zip(
                        execute(admin ->
                                admin.listConsumerGroupOffsets(offsetSpecs).all()),
                        listOffsets(topicPartitions, OffsetSpec.earliest()),
                        listOffsets(topicPartitions, OffsetSpec.latest()))
                .flatMapIterable(it -> createTopicPartitionGroupOffsets(groupIds, it.getT1(), it.getT2(), it.getT3()));
    }

    /**
     * Retrieve the offsets for the provided {@link TopicPartition}s that match the provided
     * {@link OffsetSpec}.
     *
     * @param topicPartitions The {@link TopicPartition}s to describe offsets for
     * @param offsetSpec      The criteria used to describe offsets for
     * @return A {@link Mono} of a single {@link Map} containing offsets for the provided {@link TopicPartition}s
     */
    public Mono<Map<TopicPartition, Long>> listOffsets(
            Collection<TopicPartition> topicPartitions, OffsetSpec offsetSpec) {
        return listOffsets(topicPartitions.stream().collect(Collectors.toMap(Function.identity(), __ -> offsetSpec)));
    }

    /**
     * Retrieve the offsets that match each {@link TopicPartition}'s mapped {@link OffsetSpec}
     *
     * @param offsetSpecs The mapped {@link OffsetSpec} for each {@link TopicPartition} to retrieve
     * @return A {@link Mono} of a single {@link Map} containing offsets for the provided {@link TopicPartition}s
     */
    public Mono<Map<TopicPartition, Long>> listOffsets(Map<TopicPartition, OffsetSpec> offsetSpecs) {
        return listOffsets(offsetSpecs, LongUnaryOperator.identity());
    }

    /**
     * Retrieve the offsets that match each {@link TopicPartition}'s mapped {@link OffsetSpec},
     * while applying an "adjustment" to each offset before collecting it to a result.
     *
     * @param offsetSpecs The mapped {@link OffsetSpec} for each {@link TopicPartition} to retrieve
     * @param adjustment  Modification to make to offset before collecting it to returned result
     * @return A {@link Mono} of a single {@link Map} containing offsets for the provided {@link TopicPartition}s
     */
    public Mono<Map<TopicPartition, Long>> listOffsets(
            Map<TopicPartition, OffsetSpec> offsetSpecs, LongUnaryOperator adjustment) {
        if (offsetSpecs.isEmpty()) {
            return Mono.empty();
        } else {
            return execute(admin ->
                            admin.listOffsets(offsetSpecs, LIST_OFFSETS_OPTIONS).all())
                    .flatMapIterable(Map::entrySet)
                    .collectMap(
                            Map.Entry::getKey,
                            it -> adjustment.applyAsLong(it.getValue().offset()));
        }
    }

    /**
     * Describes the existing partitions for the provided topic.
     *
     * @param topic The name of the topic to describe partitions for
     * @return A {@link Flux} of {@link TopicPartition} for each of the topic's partitions
     */
    public Flux<TopicPartition> listTopicPartitions(String topic) {
        return listTopicPartitions(Collections.singletonList(topic));
    }

    /**
     * Describes the existing partitions for the provided topics.
     *
     * @param topics The name of the topics to describe partitions for
     * @return A {@link Flux} of {@link TopicPartition} for each of the topics' partitions
     */
    public Flux<TopicPartition> listTopicPartitions(Collection<String> topics) {
        return execute(admin -> admin.describeTopics(topics).allTopicNames())
                .flatMapIterable(Map::values)
                .flatMapIterable(ReactiveAdmin::extractTopicPartitions);
    }

    @Override
    public void close() {
        admin.close();
    }

    private Flux<TopicPartitionGroupOffsets> listTopicPartitionGroupOffsets(
            Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsByGroup) {
        Set<TopicPartition> topicPartitions = offsetsByGroup.values().stream()
                .flatMap(it -> it.keySet().stream())
                .collect(Collectors.toSet());
        return listOffsets(topicPartitions, OffsetSpec.latest())
                .flatMapIterable(it -> createTopicPartitionGroupOffsets(offsetsByGroup, it));
    }

    private <T> Mono<T> execute(Function<Admin, KafkaFuture<T>> method) {
        return Mono.create(sink -> method.apply(admin).whenComplete(new SinkKafkaBiConsumer<>(sink)));
    }

    private static List<TopicPartition> extractTopicPartitions(TopicDescription topicDescription) {
        return topicDescription.partitions().stream()
                .map(it -> new TopicPartition(topicDescription.name(), it.partition()))
                .collect(Collectors.toList());
    }

    private static List<TopicPartitionGroupOffsets> createTopicPartitionGroupOffsets(
            Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsByGroup,
            Map<TopicPartition, Long> latestOffsets) {
        return offsetsByGroup.entrySet().stream()
                .flatMap(it -> createTopicPartitionGroupOffsets(it.getKey(), it.getValue(), latestOffsets).stream())
                .collect(Collectors.toList());
    }

    private static List<TopicPartitionGroupOffsets> createTopicPartitionGroupOffsets(
            String groupId,
            Map<TopicPartition, OffsetAndMetadata> groupOffsets,
            Map<TopicPartition, Long> latestOffsets) {
        return latestOffsets.entrySet().stream()
                .filter(it -> groupOffsets.get(it.getKey()) != null) // Values may be explicitly set to null
                .map(it -> new TopicPartitionGroupOffsets(
                        it.getKey(),
                        it.getValue(),
                        groupId,
                        groupOffsets.get(it.getKey()).offset()))
                .collect(Collectors.toList());
    }

    private static List<TopicPartitionGroupOffsets> createTopicPartitionGroupOffsets(
            Map<String, OffsetResetStrategy> groupIds,
            Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsByGroup,
            Map<TopicPartition, Long> earliestOffsets,
            Map<TopicPartition, Long> latestOffsets) {
        List<TopicPartitionGroupOffsets> result = new ArrayList<>();
        for (String groupId : groupIds.keySet()) {
            OffsetResetStrategy resetStrategy = groupIds.get(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    offsetsByGroup.getOrDefault(groupId, Collections.emptyMap());
            for (TopicPartition topicPartition : latestOffsets.keySet()) {
                long earliestOffset = earliestOffsets.get(topicPartition);
                long latestOffset = latestOffsets.get(topicPartition);
                calculateGroupOffset(topicPartition, offsets, resetStrategy, earliestOffset, latestOffset)
                        .map(it -> new TopicPartitionGroupOffsets(topicPartition, latestOffset, groupId, it))
                        .ifPresent(result::add);
            }
        }
        return result;
    }

    private static Optional<Long> calculateGroupOffset(
            TopicPartition topicPartition,
            Map<TopicPartition, OffsetAndMetadata> committedOffsets,
            OffsetResetStrategy resetStrategy,
            long earliestOffset,
            long latestOffset) {
        OffsetAndMetadata committedOffsetAndMetadata = committedOffsets.get(topicPartition);
        if (committedOffsetAndMetadata != null) {
            return Optional.of(committedOffsetAndMetadata.offset());
        } else if (resetStrategy == OffsetResetStrategy.EARLIEST) {
            return Optional.of(earliestOffset);
        } else if (resetStrategy == OffsetResetStrategy.LATEST) {
            return Optional.of(latestOffset);
        } else {
            return Optional.empty();
        }
    }

    private static final class SinkKafkaBiConsumer<T> implements KafkaFuture.BiConsumer<T, Throwable> {

        private final MonoSink<T> sink;

        public SinkKafkaBiConsumer(MonoSink<T> sink) {
            this.sink = sink;
        }

        @Override
        public void accept(T t, Throwable throwable) {
            if (throwable != null) {
                sink.error(throwable);
            }
            // These are no-op's if an error was previously signaled
            if (t != null) {
                sink.success(t);
            } else {
                sink.success();
            }
        }
    }
}
