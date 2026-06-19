package io.atleon.kafka;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetMetadataEncodingTest {

    private final OffsetMetadataEncoding encoding = new OffsetMetadataEncoding(Integer.MAX_VALUE);

    @Test
    public void encode_givenEmptyOffsets_producesEmptyString() {
        assertEquals("", encoding.encode(-1L, new Offsets()));
    }

    @Test
    public void decode_givenEmptyString_producesEmptyOffsets() {
        Offsets decoded = encoding.decode(-1L, "");

        assertTrue(decoded.isEmpty());
    }

    @Test
    public void encodeDecode_givenSingleOffset_roundTrips() {
        assertRoundTrips(-1L, 0L);
    }

    @Test
    public void encodeDecode_givenContiguousOffsets_roundTrips() {
        // A long run of consecutive offsets exercises the RUN_MARKER (count >= 3) encoding path
        assertRoundTrips(-1L, 0L, 1L, 2L, 3L, 4L, 5L, 6L);
    }

    @Test
    public void encodeDecode_givenRunOfLengthTwo_roundTrips() {
        // Two consecutive equal deltas exercise the count == 2 encoding path (no RUN_MARKER)
        assertRoundTrips(0L, 1L, 2L);
    }

    @Test
    public void encodeDecode_givenGappedOffsets_roundTrips() {
        assertRoundTrips(-1L, 0L, 1L, 5L, 6L, 7L, 10L);
    }

    @Test
    public void encodeDecode_givenAlternatingDeltas_roundTrips() {
        // Alternating deltas force many single-count delta-runs
        assertRoundTrips(-1L, 0L, 1L, 3L, 4L, 6L, 7L, 9L, 10L);
    }

    @Test
    public void encodeDecode_givenLargeDeltasAndBaseOffset_roundTrips() {
        // Large values exercise multi-byte VarLong encoding for deltas, counts, and the base offset
        assertRoundTrips(1_000_000L, 1_000_001L, 2_500_000L, 2_500_001L, 2_500_002L, 9_999_999_999L);
    }

    @Test
    public void encodeDecode_givenManyConsecutiveOffsets_roundTrips() {
        long base = 42L;
        long[] offsets = new long[5000];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = base + 1 + i;
        }
        assertRoundTrips(base, offsets);
    }

    @Test
    public void encode_givenBaseOffsetEqualToFirstOffset_throws() {
        Offsets offsets = build(5L, 6L);

        assertThrows(IllegalArgumentException.class, () -> encoding.encode(5L, offsets));
    }

    @Test
    public void encode_givenBaseOffsetAboveFirstOffset_throws() {
        Offsets offsets = build(5L, 6L);

        assertThrows(IllegalArgumentException.class, () -> encoding.encode(10L, offsets));
    }

    @Test
    public void decode_givenInvalidBase64_producesEmptyOffsets() {
        Offsets decoded = encoding.decode(-1L, "not valid base64 !!!");

        assertTrue(decoded.isEmpty());
    }

    @Test
    public void decode_givenMismatchedBaseOffset_producesEmptyOffsets() {
        // The base offset seeds the checksum, so decoding against a different base fails validation
        String encoded = encoding.encode(-1L, build(0L, 1L, 2L));

        Offsets decoded = encoding.decode(5L, encoded);

        assertTrue(decoded.isEmpty());
    }

    @Test
    public void decode_givenCorruptedMetadata_producesEmptyOffsets() {
        String encoded = encoding.encode(-1L, build(0L, 1L, 2L));
        String corrupted = mutateLastCharacter(encoded);

        assertNotEquals(encoded, corrupted);
        assertTrue(encoding.decode(-1L, corrupted).isEmpty());
    }

    @Test
    public void encode_givenInsufficientMaxSize_producesEmptyString() {
        OffsetMetadataEncoding tinyEncoding = new OffsetMetadataEncoding(1);

        assertEquals("", tinyEncoding.encode(-1L, build(0L, 1L, 2L)));
    }

    @Test
    public void encodeDecode_givenMaxSizeBelowFullSize_truncatesToPrefix() {
        OffsetMetadataEncoding boundedEncoding = new OffsetMetadataEncoding(200);

        // Alternating deltas yield mostly single-count runs (~1 byte each), exceeding the limit
        long[] offsets = new long[2000];
        long previous = -1L;
        for (int i = 0; i < offsets.length; i++) {
            previous += (i % 2 == 0) ? 1L : 2L;
            offsets[i] = previous;
        }
        Offsets full = build(offsets);

        String encoded = boundedEncoding.encode(-1L, full);
        Offsets decoded = boundedEncoding.decode(-1L, encoded);

        assertFalse(decoded.isEmpty());
        assertTrue(decoded.size() < full.size());
        // Whatever survives truncation must be a leading prefix of the original offsets
        assertEquals(toList(full).subList(0, decoded.size()), toList(decoded));
    }

    private void assertRoundTrips(long baseOffset, long... offsets) {
        Offsets original = build(offsets);

        String encoded = encoding.encode(baseOffset, original);
        Offsets decoded = encoding.decode(baseOffset, encoded);

        assertEquals(toList(original), toList(decoded));
        assertEquals(original.size(), decoded.size());
    }

    private static String mutateLastCharacter(String encoded) {
        char[] chars = encoded.toCharArray();
        int lastIndex = chars.length - 1;
        chars[lastIndex] = chars[lastIndex] == 'A' ? 'B' : 'A';
        return new String(chars);
    }

    private static Offsets build(long... offsets) {
        Offsets.Builder builder = new Offsets.Builder();
        for (long offset : offsets) {
            builder.addNext(offset);
        }
        return builder.build();
    }

    private static List<Long> toList(Offsets offsets) {
        List<Long> result = new ArrayList<>();
        offsets.iterator().forEachRemaining(result::add);
        return result;
    }
}
