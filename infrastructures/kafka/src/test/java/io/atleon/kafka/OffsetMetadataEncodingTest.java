package io.atleon.kafka;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetMetadataEncodingTest {

    @Test
    public void serializeProcessed_givenSingleOffsetAtCommitOffset_expectsRoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(5L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(5L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(5L, serialized, Integer.MAX_VALUE);

        assertEquals(Collections.singletonList(5L), deserialized);
    }

    @Test
    public void serializeProcessed_givenContiguousOffsets_expectsRoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 11L, 12L, 13L, 14L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, Integer.MAX_VALUE);

        assertEquals(new ArrayList<>(processedOffsets), deserialized);
    }

    @Test
    public void serializeProcessed_givenTwoContiguousOffsets_expectsRoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(5L, 6L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(5L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(5L, serialized, Integer.MAX_VALUE);

        assertEquals(Arrays.asList(5L, 6L), deserialized);
    }

    @Test
    public void serializeProcessed_givenSparseOffsets_expectsRoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 13L, 14L, 20L, 100L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, Integer.MAX_VALUE);

        assertEquals(new ArrayList<>(processedOffsets), deserialized);
    }

    @Test
    public void serializeProcessed_givenMixedRunsAndSingles_expectsRoundTrip() {
        // A run of three contiguous, then a gap, then a single.
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 11L, 12L, 20L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, Integer.MAX_VALUE);

        assertEquals(Arrays.asList(10L, 11L, 12L, 20L), deserialized);
    }

    @Test
    public void serializeProcessed_givenGapBetweenCommitAndFirstOffset_expectsRoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(15L, 16L, 17L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, Integer.MAX_VALUE);

        assertEquals(Arrays.asList(15L, 16L, 17L), deserialized);
    }

    @Test
    public void serializeProcessed_givenLargeOffsetsRequiringMultiByteVarLong_expectsRoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(1_000_000_000L, 1_000_000_001L, 2_000_000_000L);

        byte[] serialized =
                OffsetMetadataEncoding.serializeProcessed(1_000_000_000L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized =
                OffsetMetadataEncoding.deserializeProcessed(1_000_000_000L, serialized, Integer.MAX_VALUE);

        assertEquals(new ArrayList<>(processedOffsets), deserialized);
    }

    @Test
    public void serializeProcessed_givenLongRunOfContiguousOffsets_expectsCompactRoundTrip() {
        SortedSet<Long> processedOffsets = new TreeSet<>();
        for (long offset = 0L; offset < 1000L; offset++) {
            processedOffsets.add(offset);
        }

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(0L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(0L, serialized, Integer.MAX_VALUE);

        assertEquals(new ArrayList<>(processedOffsets), deserialized);
        // Run-length encoding keeps a 1000-offset contiguous run far smaller than one byte per offset.
        assertTrue(serialized.length < 50, "Expected compact run encoding but was " + serialized.length + " bytes");
    }

    @Test
    public void serializeProcessed_givenEmptyOffsets_expectsEmptyOutput() {
        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(5L, new TreeSet<>(), Integer.MAX_VALUE);

        assertEquals(0, serialized.length);
        assertTrue(OffsetMetadataEncoding.deserializeProcessed(5L, serialized, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void serializeProcessed_givenZeroLimit_expectsEmptyOutput() {
        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(5L, sortedSetOf(5L, 6L, 7L), 0);

        assertEquals(0, serialized.length);
        assertTrue(OffsetMetadataEncoding.deserializeProcessed(5L, serialized, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void serializeProcessed_givenLimitSmallerThanSet_expectsOnlySmallestOffsets() {
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 11L, 12L, 13L, 14L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, 3);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, Integer.MAX_VALUE);

        assertEquals(Arrays.asList(10L, 11L, 12L), deserialized);
    }

    @Test
    public void deserializeProcessed_givenLimitSmallerThanEncoded_expectsOnlySmallestOffsets() {
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 11L, 12L, 13L, 14L);

        // Encoded under a larger honorship/limit than the limit used to decode (e.g. config lowered between runs).
        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, 3);

        assertEquals(Arrays.asList(10L, 11L, 12L), deserialized);
    }

    @Test
    public void deserializeProcessed_givenLimitSmallerThanEncodedSparseOffsets_expectsOnlySmallestOffsets() {
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 13L, 14L, 20L, 100L);

        // Exercises the limit boundary falling within a single-delta region rather than a run.
        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, 2);

        assertEquals(Arrays.asList(10L, 13L), deserialized);
    }

    @Test
    public void deserializeProcessed_givenLimitTruncatingWithinRunFollowedBySingles_expectsOnlySmallestOffsets() {
        // A run of four contiguous offsets, then two sparse singles. Limiting within the run forces decoding to
        // keep consuming the trailing singles (advancing to the CRC) even though no further offsets are retained.
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 11L, 12L, 13L, 20L, 30L);

        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> deserialized = OffsetMetadataEncoding.deserializeProcessed(10L, serialized, 2);

        assertEquals(Arrays.asList(10L, 11L), deserialized);
    }

    @Test
    public void serializeProcessed_givenCommitOffsetGreaterThanFirstProcessed_expectsException() {
        SortedSet<Long> processedOffsets = sortedSetOf(5L, 6L);

        assertThrows(
                IllegalArgumentException.class,
                () -> OffsetMetadataEncoding.serializeProcessed(6L, processedOffsets, Integer.MAX_VALUE));
    }

    @Test
    public void deserializeProcessed_givenEmptyBytes_expectsEmptyList() {
        assertTrue(OffsetMetadataEncoding.deserializeProcessed(5L, new byte[0], Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void deserializeProcessed_givenCorruptedBytes_expectsEmptyList() {
        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(5L, sortedSetOf(5L, 6L, 7L), Integer.MAX_VALUE);

        // Corrupt the trailing CRC so validation fails.
        serialized[serialized.length - 1] ^= 0x01;

        assertTrue(OffsetMetadataEncoding.deserializeProcessed(5L, serialized, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void deserializeProcessed_givenTrailingBytes_expectsEmptyList() {
        byte[] serialized = OffsetMetadataEncoding.serializeProcessed(5L, sortedSetOf(5L, 6L, 7L), Integer.MAX_VALUE);

        byte[] withTrailing = Arrays.copyOf(serialized, serialized.length + 1);

        assertTrue(OffsetMetadataEncoding.deserializeProcessed(5L, withTrailing, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void deserializeProcessed_givenMismatchedCommitOffset_expectsEmptyList() {
        byte[] serialized =
                OffsetMetadataEncoding.serializeProcessed(100L, sortedSetOf(100L, 101L, 102L), Integer.MAX_VALUE);

        // The CRC is seeded with the commit offset, so decoding under a different commit offset fails validation.
        assertTrue(OffsetMetadataEncoding.deserializeProcessed(50L, serialized, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void deserializeProcessed_givenIncompatibleVersion_expectsEmptyList() {
        // A single varlong byte decoding to version 2, which is incompatible with the supported version.
        byte[] incompatibleVersion = new byte[] {0x02};

        assertTrue(OffsetMetadataEncoding.deserializeProcessed(5L, incompatibleVersion, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void encodeProcessed_givenOffsets_expectsBase64RoundTrip() {
        SortedSet<Long> processedOffsets = sortedSetOf(10L, 11L, 13L);

        String encoded = OffsetMetadataEncoding.encodeProcessed(10L, processedOffsets, Integer.MAX_VALUE);
        List<Long> decoded = OffsetMetadataEncoding.decodeProcessed(10L, encoded, Integer.MAX_VALUE);

        assertEquals(new ArrayList<>(processedOffsets), decoded);
    }

    @Test
    public void encodeProcessed_givenEmptyOffsets_expectsEmptyEncoding() {
        String encoded = OffsetMetadataEncoding.encodeProcessed(5L, new TreeSet<>(), Integer.MAX_VALUE);

        assertEquals("", encoded);
        assertTrue(OffsetMetadataEncoding.decodeProcessed(5L, encoded, Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void decodeProcessed_givenInvalidBase64_expectsEmptyList() {
        assertTrue(OffsetMetadataEncoding.decodeProcessed(5L, "not valid base64!!!", Integer.MAX_VALUE)
                .isEmpty());
    }

    @Test
    public void encodeProcessed_givenWithoutPadding_expectsNoPaddingCharacters() {
        String encoded = OffsetMetadataEncoding.encodeProcessed(10L, sortedSetOf(10L, 11L, 12L), Integer.MAX_VALUE);

        assertNotEquals("", encoded);
        assertTrue(encoded.indexOf('=') < 0, "Expected no Base64 padding but was: " + encoded);
    }

    private static SortedSet<Long> sortedSetOf(Long... offsets) {
        return new TreeSet<>(Arrays.asList(offsets));
    }
}
