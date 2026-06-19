package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OffsetTrackerTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);

    @Test
    public void acknowledgedAheadOfCommit_givenNoPriorMetadata_expectsNoneProhibited() {
        OffsetTracker tracker = newAcknowledgedAheadOfCommitTracker(5L, null);

        assertFalse(tracker.prohibitsProcessing(5L));
        assertFalse(tracker.prohibitsProcessing(6L));
    }

    @Test
    public void acknowledgedAheadOfCommit_givenOffsetsAcknowledgedInPriorAssignment_expectsThemProhibited() {
        // A prior assignment acknowledged offsets 6 and 8 while the commit offset remained at 5
        // (offsets 5 and 7 were never acknowledged), and persisted that state to commit metadata.
        OffsetTracker priorTracker = newAcknowledgedAheadOfCommitTracker(5L, null);
        priorTracker.acknowledged(6L);
        priorTracker.acknowledged(8L);
        String metadata = priorTracker.commitMetadata(5L).block();

        // The next assignment resumes from the same position with that metadata restored
        OffsetTracker resumedTracker = newAcknowledgedAheadOfCommitTracker(5L, metadata);

        assertTrue(resumedTracker.prohibitsProcessing(6L));
        assertTrue(resumedTracker.prohibitsProcessing(8L));
        assertFalse(resumedTracker.prohibitsProcessing(5L));
        assertFalse(resumedTracker.prohibitsProcessing(7L));
        assertFalse(resumedTracker.prohibitsProcessing(9L));
    }

    @Test
    public void acknowledgedAheadOfCommit_givenNoAcknowledgedOffsets_expectsBlankMetadata() {
        OffsetTracker tracker = newAcknowledgedAheadOfCommitTracker(5L, null);

        // Blank (not absent) metadata means native commitment still happens, just with no offsets
        assertEquals("", tracker.commitMetadata(5L).block());
    }

    @Test
    public void acknowledgedAheadOfCommit_givenAcknowledgedOffsets_expectsOnlyOffsetsAtOrAboveCommitRetained() {
        OffsetTracker tracker = newAcknowledgedAheadOfCommitTracker(0L, null);
        tracker.acknowledged(2L);
        tracker.acknowledged(5L);
        tracker.acknowledged(8L);

        // Committing at offset 6 drops acknowledged offsets below it (2 and 5) and retains 8
        String metadata = tracker.commitMetadata(6L).block();
        OffsetTracker resumedTracker = newAcknowledgedAheadOfCommitTracker(6L, metadata);
        assertTrue(resumedTracker.prohibitsProcessing(8L));
        assertFalse(resumedTracker.prohibitsProcessing(2L));
        assertFalse(resumedTracker.prohibitsProcessing(5L));

        // The dropped offsets are gone for good; a later commit cannot resurrect them
        String laterMetadata = tracker.commitMetadata(0L).block();
        OffsetTracker laterResumedTracker = newAcknowledgedAheadOfCommitTracker(0L, laterMetadata);
        assertTrue(laterResumedTracker.prohibitsProcessing(8L));
        assertFalse(laterResumedTracker.prohibitsProcessing(2L));
        assertFalse(laterResumedTracker.prohibitsProcessing(5L));
    }

    @Test
    public void acknowledgedAheadOfCommit_givenInitialPosition_expectsInitialConsumerOffset() {
        OffsetTracker tracker = newAcknowledgedAheadOfCommitTracker(5L, null);

        assertEquals(
                4L,
                tracker.initialConsumerOffset().orElseThrow(AssertionError::new).offset());
    }

    private static OffsetTracker newAcknowledgedAheadOfCommitTracker(long position, String committedMetadata) {
        return OffsetTracker.acknowledgedAheadOfCommit(consumerPartition(position, committedMetadata));
    }

    private static ConsumerPartition consumerPartition(long position, String committedMetadata) {
        Consumer<?, ?> consumer = mock(Consumer.class);
        when(consumer.position(TOPIC_PARTITION)).thenReturn(position);
        OffsetAndMetadata committed =
                committedMetadata == null ? null : new OffsetAndMetadata(position, committedMetadata);
        when(consumer.committed(anySet())).thenReturn(Collections.singletonMap(TOPIC_PARTITION, committed));
        return ConsumerPartition.create(consumer, Collections.singletonList(TOPIC_PARTITION))
                .get(0);
    }
}
