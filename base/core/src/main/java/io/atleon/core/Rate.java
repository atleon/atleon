package io.atleon.core;

import java.time.Duration;
import java.util.Objects;

/**
 * Represents unitless count over a discrete {@link Duration} of time.
 */
public final class Rate {

    private final long count;

    private final Duration duration;

    private Rate(long count, Duration duration) {
        if (count < 0L) {
            throw new IllegalArgumentException("Count must be non-negative: " + count);
        }
        this.count = count;
        this.duration = duration;
    }

    public static Rate zero() {
        return new Rate(0L, Duration.ofNanos(1));
    }

    public static Rate infinite() {
        return new Rate(Long.MAX_VALUE, Duration.ofNanos(1));
    }

    /**
     * Creates a new {@link Rate} with second denomination.
     */
    public static Rate perSecond(long count) {
        return new Rate(count, Duration.ofSeconds(1));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Rate that = (Rate) o;
        return count == that.count && Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, duration);
    }

    @Override
    public String toString() {
        return "Throughput{" + "count=" + count + ", duration=" + duration + '}';
    }

    public boolean isZero() {
        return count == 0L;
    }

    public boolean isInfinite() {
        return count == Long.MAX_VALUE;
    }

    public long count() {
        return count;
    }

    public Duration duration() {
        return duration;
    }
}
