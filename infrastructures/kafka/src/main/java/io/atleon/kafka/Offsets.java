package io.atleon.kafka;

import org.jspecify.annotations.NonNull;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.Collectors;

/**
 * A {@link Collection} of {@code long} offsets backed by a sorted {@link List} of contiguous
 * "ranges". Storing runs of consecutive offsets as a single range (rather than one entry per
 * offset) keeps memory proportional to the number of <i>gaps</i> rather than the number of
 * offsets, which suits intended usage with mostly-contiguous acknowledged offsets. Offsets are
 * iterated in ascending order and duplicates are not retained.
 *
 * <p>The sole available "mutation" is {@link #minimumMerge(long, Collection)}, which returns a
 * <i>new</i> instance by combining the contained offsets with additional offsets while discarding
 * any below a given floor (i.e. the commit offset).
 */
final class Offsets extends AbstractCollection<Long> {

    // More performant usable value for representing "null" without boxing.
    private static final long NO_VALUE = Long.MIN_VALUE;

    // Sorted ascending by start, non-overlapping, and non-adjacent (there is always a gap of at
    // least one offset between consecutive ranges; otherwise they would have been consolidated).
    private final List<Range> ranges;

    // Precomputed to avoid unnecessary traversal.
    private final int offsetCount;

    public Offsets() {
        this.ranges = Collections.emptyList();
        this.offsetCount = 0;
    }

    private Offsets(ArrayList<Range> ranges, int offsetCount) {
        this.ranges = ranges;
        this.offsetCount = offsetCount;
    }

    public Offsets minimumMerge(long minimum, Collection<Long> sortedAdditionalOffsets) {
        Builder builder = new Builder();
        MergingIterator mergingIterator = new MergingIterator(longIterator(), sortedAdditionalOffsets.iterator());
        while (mergingIterator.hasNext()) {
            long nextOffset = mergingIterator.nextLong();
            if (nextOffset >= minimum) {
                builder.addNext(nextOffset);
            }
        }
        return builder.build();
    }

    @Override
    public boolean contains(Object o) {
        return o != null && o.getClass() == Long.class && contains(((Long) o).longValue());
    }

    public boolean contains(long offset) {
        // Fast path: Most checks will be for offsets that are greater than current max
        int lastIndex = ranges.size() - 1;
        if (lastIndex < 0 || ranges.get(lastIndex).end() < offset) {
            return false;
        }

        int floorIndex = -1;
        int low = 0;
        int high = lastIndex;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (ranges.get(mid).start() <= offset) {
                floorIndex = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return floorIndex >= 0 && offset <= ranges.get(floorIndex).end();
    }

    @Override
    public int size() {
        return offsetCount;
    }

    @Override
    public @NonNull Iterator<Long> iterator() {
        return longIterator();
    }

    public PrimitiveIterator.OfLong longIterator() {
        return new PrimitiveIterator.OfLong() {

            private int rangeIndex = 0;

            private long next = ranges.isEmpty() ? 0 : ranges.get(0).start();

            @Override
            public boolean hasNext() {
                return rangeIndex < ranges.size();
            }

            @Override
            public long nextLong() {
                long result = next;

                if (result != ranges.get(rangeIndex).end()) {
                    next = result + 1;
                } else if (++rangeIndex < ranges.size()) {
                    next = ranges.get(rangeIndex).start();
                }

                return result;
            }
        };
    }

    @Override
    public String toString() {
        return ranges.stream().map(Range::toString).collect(Collectors.joining(", ", "[", "]"));
    }

    static final class Builder {

        private final List<Range> ranges = new LinkedList<>();

        private long startOffset = NO_VALUE;

        private long previousOffset = NO_VALUE;

        private int offsetCount = 0;

        public Offsets build() {
            if (startOffset != NO_VALUE) {
                ArrayList<Range> builtRanges = new ArrayList<>(ranges.size() + 1);
                builtRanges.addAll(ranges);
                builtRanges.add(new Range(startOffset, previousOffset));
                return new Offsets(builtRanges, offsetCount);
            } else {
                return new Offsets();
            }
        }

        public void addNext(long offset) {
            if (startOffset == NO_VALUE) {
                startOffset = offset;
            } else if (offset > previousOffset + 1) {
                ranges.add(new Range(startOffset, previousOffset));
                startOffset = offset;
            } else if (offset < previousOffset) {
                throw new IllegalArgumentException("Offsets to add must be done so in sorted order");
            }
            if (offset != previousOffset) {
                previousOffset = offset;
                offsetCount++;
            }
        }
    }

    private static final class MergingIterator implements PrimitiveIterator.OfLong {

        private final PrimitiveIterator.OfLong left;

        private final Iterator<Long> right;

        private long nextLeft;

        private long nextRight;

        public MergingIterator(PrimitiveIterator.OfLong left, Iterator<Long> right) {
            this.left = left;
            this.right = right;
            this.nextLeft = left.hasNext() ? left.nextLong() : NO_VALUE;
            this.nextRight = right.hasNext() ? right.next() : NO_VALUE;
        }

        @Override
        public boolean hasNext() {
            return nextLeft != NO_VALUE || nextRight != NO_VALUE;
        }

        @Override
        public long nextLong() {
            if (nextLeft == NO_VALUE) {
                return getAndAdvanceRight();
            } else if (nextRight == NO_VALUE || nextLeft <= nextRight) {
                return getAndAdvanceLeft();
            } else {
                return getAndAdvanceRight();
            }
        }

        private long getAndAdvanceLeft() {
            long result = nextLeft;
            nextLeft = left.hasNext() ? left.nextLong() : NO_VALUE;
            return result;
        }

        private long getAndAdvanceRight() {
            long result = nextRight;
            nextRight = right.hasNext() ? right.next() : NO_VALUE;
            return result;
        }
    }

    private static final class Range {

        private final long start;

        private final long end;

        public Range(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return start == end ? Long.toString(start) : (start + ".." + end);
        }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }
    }
}
