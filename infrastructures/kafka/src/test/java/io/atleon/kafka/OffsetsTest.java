package io.atleon.kafka;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetsTest {

    @Test
    public void newInstance_isEmpty() {
        Offsets offsets = new Offsets();

        assertTrue(offsets.isEmpty());
        assertEquals(0, offsets.size());
        assertFalse(offsets.contains(0L));
        assertFalse(offsets.iterator().hasNext());
    }

    @Test
    public void builder_givenContiguousOffsets_consolidatesIntoSingleRange() {
        Offsets offsets = build(0L, 1L, 2L, 3L);

        assertFalse(offsets.isEmpty());
        assertEquals(4, offsets.size());
        assertEquals(Arrays.asList(0L, 1L, 2L, 3L), toList(offsets));
        assertEquals("[0..3]", offsets.toString());
    }

    @Test
    public void builder_givenGappedOffsets_producesMultipleRanges() {
        Offsets offsets = build(0L, 1L, 5L, 6L, 7L, 10L);

        assertEquals(6, offsets.size());
        assertEquals(Arrays.asList(0L, 1L, 5L, 6L, 7L, 10L), toList(offsets));
        assertEquals("[0..1, 5..7, 10]", offsets.toString());
    }

    @Test
    public void builder_givenDuplicateOffset_retainsSingleOccurrence() {
        Offsets offsets = build(3L, 3L, 4L);

        assertEquals(2, offsets.size());
        assertEquals(Arrays.asList(3L, 4L), toList(offsets));
    }

    @Test
    public void builder_givenOutOfOrderOffsets_throws() {
        Offsets.Builder builder = new Offsets.Builder();
        builder.addNext(5L);

        assertThrows(IllegalArgumentException.class, () -> builder.addNext(4L));
    }

    @Test
    public void contains_reflectsPresenceAcrossRangesAndGaps() {
        Offsets offsets = build(0L, 1L, 5L, 6L, 7L);

        assertTrue(offsets.contains(0L));
        assertTrue(offsets.contains(1L));
        assertTrue(offsets.contains(6L));
        assertTrue(offsets.contains(7L));
        assertFalse(offsets.contains(2L));
        assertFalse(offsets.contains(4L));
        assertFalse(offsets.contains(8L));
    }

    @Test
    public void contains_givenNonLong_returnsFalse() {
        Offsets offsets = build(1L, 2L);

        assertFalse(offsets.contains((Object) "1"));
        assertFalse(offsets.contains((Object) Integer.valueOf(1)));
        assertFalse(offsets.contains(null));
    }

    @Test
    public void minimumMerge_givenAdditionalOffsets_combinesWithExisting() {
        Offsets offsets = build(1L, 2L, 5L);

        Offsets merged = offsets.minimumMerge(0L, Arrays.asList(3L, 4L, 6L));

        assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), toList(merged));
        assertEquals("[1..6]", merged.toString());
    }

    @Test
    public void minimumMerge_discardsOffsetsBelowMinimum() {
        Offsets offsets = build(1L, 2L, 3L);

        Offsets merged = offsets.minimumMerge(3L, Arrays.asList(4L, 5L));

        assertEquals(Arrays.asList(3L, 4L, 5L), toList(merged));
    }

    @Test
    public void minimumMerge_givenAllOffsetsBelowMinimum_returnsEmpty() {
        Offsets offsets = build(1L, 2L, 3L);

        Offsets merged = offsets.minimumMerge(10L, Collections.emptyList());

        assertTrue(merged.isEmpty());
    }

    @Test
    public void minimumMerge_dedupesOverlapBetweenExistingAndAdditional() {
        Offsets offsets = build(1L, 2L, 3L);

        Offsets merged = offsets.minimumMerge(0L, Arrays.asList(2L, 3L, 4L));

        assertEquals(Arrays.asList(1L, 2L, 3L, 4L), toList(merged));
    }

    @Test
    public void minimumMerge_givenUnsortedAdditionalOffsets_throws() {
        Offsets offsets = build(1L);

        assertThrows(IllegalArgumentException.class, () -> offsets.minimumMerge(0L, Arrays.asList(5L, 3L)));
    }

    @Test
    public void minimumMerge_doesNotMutateOriginal() {
        Offsets offsets = build(1L, 2L);

        offsets.minimumMerge(0L, Arrays.asList(3L, 4L));

        assertEquals(Arrays.asList(1L, 2L), toList(offsets));
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
