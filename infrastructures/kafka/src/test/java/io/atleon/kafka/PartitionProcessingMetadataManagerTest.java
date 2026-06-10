package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionProcessingMetadataManagerTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);

    @Test
    public void create_givenNonPositiveHonorship_expectsEmptyMapWithoutQueryingCommittedOffsets() {
        Consumer<?, ?> consumer = mock(Consumer.class);

        Map<TopicPartition, PartitionProcessingMetadataManager> managers =
                PartitionProcessingMetadataManager.create(0, consumer, Collections.singletonList(TOPIC_PARTITION));

        assertTrue(managers.isEmpty());
        verify(consumer, never()).committed(anySet());
    }

    @Test
    public void create_givenEmptyPartitions_expectsEmptyMapWithoutQueryingCommittedOffsets() {
        Consumer<?, ?> consumer = mock(Consumer.class);

        Map<TopicPartition, PartitionProcessingMetadataManager> managers =
                PartitionProcessingMetadataManager.create(10, consumer, Collections.emptyList());

        assertTrue(managers.isEmpty());
        verify(consumer, never()).committed(anySet());
    }

    @Test
    public void create_givenMultiplePartitions_expectsManagerForEachPartition() {
        TopicPartition otherPartition = new TopicPartition("topic", 1);
        Consumer<?, ?> consumer = mockConsumerWithCommitted(0L, Collections.emptyMap());

        Map<TopicPartition, PartitionProcessingMetadataManager> managers =
                PartitionProcessingMetadataManager.create(10, consumer, Arrays.asList(TOPIC_PARTITION, otherPartition));

        assertEquals(2, managers.size());
        assertTrue(managers.containsKey(TOPIC_PARTITION));
        assertTrue(managers.containsKey(otherPartition));
    }

    @Test
    public void
            create_givenNoCommittedOffsets_expectsNoPreviouslyProcessedOffsetsButInitialAcknowledgeableOffsetReported() {
        // No committed offset exists for the partition, but the consumer is still positioned (e.g.
        // via offset reset). The initial acknowledgeable offset is the offset just before that
        // position, not the absent committed offset, while there are no previously processed
        // offsets to decode.
        Consumer<?, ?> consumer = mockConsumerWithCommitted(50L, Collections.emptyMap());

        PartitionProcessingMetadataManager manager = PartitionProcessingMetadataManager.create(
                        10, consumer, Collections.singletonList(TOPIC_PARTITION))
                .get(TOPIC_PARTITION);

        assertEquals(Optional.of(49L), manager.initialAcknowledgeableOffset());
        assertFalse(manager.previouslyProcessed(50L));
    }

    @Test
    public void create_givenPositionDistinctFromCommittedMetadataBasis_expectsMetadataValidatedAgainstPosition() {
        // The committed metadata was encoded against the committed offset (10), but the consumer is
        // now positioned at 13. Because decoding validates against the initial position rather than
        // the committed offset, the metadata fails validation and none of its offsets are reported
        // as previously processed, even though they are present in the committed metadata.
        PartitionProcessingMetadataManager manager = createManager(10, 13L, 10L, Arrays.asList(10L, 11L, 15L));

        assertEquals(Optional.of(12L), manager.initialAcknowledgeableOffset());
        assertFalse(manager.previouslyProcessed(10L));
        assertFalse(manager.previouslyProcessed(11L));
        assertFalse(manager.previouslyProcessed(15L));
    }

    @Test
    public void create_givenCommittedMetadata_expectsDecodedOffsetsReportedAsPreviouslyProcessed() {
        // The consumer is positioned at the offset the committed metadata was encoded against (10),
        // so the metadata validates against the initial position and its offsets are honored. The
        // initial acknowledgeable offset is the offset just before that position.
        PartitionProcessingMetadataManager manager = createManager(10, 10L, Arrays.asList(10L, 11L, 15L));

        assertEquals(Optional.of(9L), manager.initialAcknowledgeableOffset());

        // Offsets encoded into the committed metadata are reported as previously processed.
        assertTrue(manager.previouslyProcessed(10L));
        assertTrue(manager.previouslyProcessed(11L));
        assertTrue(manager.previouslyProcessed(15L));

        // Offsets within the encoded range but absent from it are not previously processed.
        assertFalse(manager.previouslyProcessed(12L));

        // Offsets beyond the largest encoded offset are not previously processed.
        assertFalse(manager.previouslyProcessed(16L));

        // Offsets below the smallest encoded offset are not previously processed.
        assertFalse(manager.previouslyProcessed(9L));
    }

    @Test
    public void previouslyProcessed_givenEmptyPreviouslyProcessed_expectsAlwaysFalse() {
        PartitionProcessingMetadataManager manager = createManager(10, 10L, Collections.emptyList());

        assertFalse(manager.previouslyProcessed(0L));
        assertFalse(manager.previouslyProcessed(10L));
    }

    @Test
    public void previouslyProcessed_givenOffsetMarkedProcessedAfterCreation_expectsStillFalse() {
        PartitionProcessingMetadataManager manager = createManager(10, 10L, Collections.emptyList());

        // Marking an offset as processed within this lifecycle does not make it "previously"
        // processed, which is determined solely from the metadata present at creation.
        manager.processed(5L);

        assertFalse(manager.previouslyProcessed(5L));
    }

    @Test
    public void updateAndGetMetadataOnCommit_givenProcessedOffsets_expectsThemEncodedIntoMetadata() {
        PartitionProcessingMetadataManager manager = createManager(10, 5L, Collections.emptyList());

        manager.processed(5L);
        manager.processed(6L);
        manager.processed(9L);

        String metadata = manager.updateAndGetMetadataOnCommit(4L);

        assertEquals(
                Arrays.asList(5L, 6L, 9L), OffsetMetadataEncoding.decodeProcessed(4L, metadata, Integer.MAX_VALUE));
    }

    @Test
    public void updateAndGetMetadataOnCommit_givenProcessedOffsetsBelowCommit_expectsThemExcluded() {
        PartitionProcessingMetadataManager manager = createManager(10, 5L, Collections.emptyList());

        manager.processed(5L);
        manager.processed(6L);
        manager.processed(7L);
        manager.processed(10L);

        // Committing at 7 drops the offsets below it; only 7 and 10 remain at or above the commit.
        String metadata = manager.updateAndGetMetadataOnCommit(7L);

        assertEquals(Arrays.asList(7L, 10L), OffsetMetadataEncoding.decodeProcessed(7L, metadata, Integer.MAX_VALUE));
    }

    @Test
    public void updateAndGetMetadataOnCommit_givenNoProcessedOffsetsAtOrAboveCommit_expectsEmptyMetadata() {
        PartitionProcessingMetadataManager manager = createManager(10, 5L, Collections.emptyList());

        manager.processed(5L);
        manager.processed(6L);

        String metadata = manager.updateAndGetMetadataOnCommit(10L);

        assertEquals("", metadata);
        assertTrue(OffsetMetadataEncoding.decodeProcessed(10L, metadata, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void updateAndGetMetadataOnCommit_givenMoreProcessedThanHonorship_expectsOnlySmallestRetained() {
        PartitionProcessingMetadataManager manager = createManager(2, 10L, Collections.emptyList());

        manager.processed(10L);
        manager.processed(11L);
        manager.processed(12L);
        manager.processed(13L);

        // The honorship of 2 limits the encoded metadata to the two smallest processed offsets.
        String metadata = manager.updateAndGetMetadataOnCommit(10L);

        assertEquals(Arrays.asList(10L, 11L), OffsetMetadataEncoding.decodeProcessed(10L, metadata, Integer.MAX_VALUE));
    }

    @Test
    public void updateAndGetMetadataOnCommit_givenPreviouslyProcessedOffsets_expectsThemCarriedForward() {
        PartitionProcessingMetadataManager manager = createManager(10, 10L, Arrays.asList(10L, 11L, 15L));

        // Without marking anything new, committing at the original offset re-encodes the previously
        // processed offsets.
        String metadata = manager.updateAndGetMetadataOnCommit(10L);

        assertEquals(
                Arrays.asList(10L, 11L, 15L), OffsetMetadataEncoding.decodeProcessed(10L, metadata, Integer.MAX_VALUE));
    }

    @Test
    public void updateAndGetMetadataOnCommit_givenRepeatedCommits_expectsRemovalsToPersist() {
        PartitionProcessingMetadataManager manager = createManager(10, 5L, Collections.emptyList());

        manager.processed(5L);
        manager.processed(6L);
        manager.processed(9L);

        // Committing at 7 permanently removes 5 and 6.
        manager.updateAndGetMetadataOnCommit(7L);

        // A subsequent commit below 7 cannot resurrect the removed offsets; only 9 remains.
        String metadata = manager.updateAndGetMetadataOnCommit(5L);

        assertEquals(
                Collections.singletonList(9L), OffsetMetadataEncoding.decodeProcessed(5L, metadata, Integer.MAX_VALUE));
    }

    @Test
    public void noOp_expectsInertBehavior() {
        PartitionProcessingMetadataManager manager = PartitionProcessingMetadataManager.noOp();

        // Marking offsets processed has no effect and does not throw.
        manager.processed(5L);

        assertEquals("", manager.updateAndGetMetadataOnCommit(5L));
        assertFalse(manager.previouslyProcessed(5L));
        assertEquals(Optional.empty(), manager.initialAcknowledgeableOffset());
    }

    private static PartitionProcessingMetadataManager createManager(
            int honorship, long committedOffset, List<Long> previouslyProcessed) {
        // In the common case, the consumer is positioned at the committed offset on assignment.
        return createManager(honorship, committedOffset, committedOffset, previouslyProcessed);
    }

    private static PartitionProcessingMetadataManager createManager(
            int honorship, long initialPosition, long committedOffset, List<Long> previouslyProcessed) {
        Map<TopicPartition, OffsetAndMetadata> committed =
                Collections.singletonMap(TOPIC_PARTITION, toOffsetAndMetadata(committedOffset, previouslyProcessed));
        Consumer<?, ?> consumer = mockConsumerWithCommitted(initialPosition, committed);
        return PartitionProcessingMetadataManager.create(honorship, consumer, committed.keySet())
                .get(TOPIC_PARTITION);
    }

    private static OffsetAndMetadata toOffsetAndMetadata(long committedOffset, List<Long> processed) {
        String metadata =
                OffsetMetadataEncoding.encodeProcessed(committedOffset, new TreeSet<>(processed), Integer.MAX_VALUE);
        return new OffsetAndMetadata(committedOffset, metadata);
    }

    private static Consumer<?, ?> mockConsumerWithCommitted(
            long initialPosition, Map<TopicPartition, OffsetAndMetadata> committed) {
        Consumer<?, ?> consumer = mock(Consumer.class);
        // The cast mirrors the production call site, which queries committed offsets for a Set of partitions.
        when(consumer.committed(anySet())).thenReturn(new HashMap<>(committed));
        // Initialization is based on the consumer's position, which is queried per assigned partition.
        when(consumer.position(TOPIC_PARTITION)).thenReturn(initialPosition);
        return consumer;
    }
}
