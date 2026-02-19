package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Convenience class for applying size and time bounded batching to {@link Publisher}s
 */
public final class Batcher {

    private final int maxSize;

    private final Duration maxDuration;

    private final int prefetch;

    private Batcher(int maxSize, Duration maxDuration, int prefetch) {
        this.maxSize = maxSize;
        this.maxDuration = maxDuration;
        this.prefetch = prefetch;
    }

    public static Batcher create(int maxSize, Duration maxDuration, int prefetch) {
        return new Batcher(maxSize, maxDuration, prefetch);
    }

    public <T, R> Flux<R> applyMapping(
            Publisher<T> publisher,
            Function<? super List<T>, ? extends Publisher<? extends R>> mapper,
            int maxConcurrency) {
        return maxConcurrency <= 1
                ? toBatches(publisher).concatMap(mapper, prefetch)
                : toBatches(publisher)
                        .publishOn(Schedulers.immediate(), prefetch)
                        .flatMap(mapper, maxConcurrency);
    }

    private <T> Flux<List<T>> toBatches(Publisher<T> publisher) {
        if (maxSize <= 1) {
            return Flux.from(publisher).map(Collections::singletonList);
        } else if (maxDuration.isZero() || maxDuration.isNegative()) {
            throw new IllegalArgumentException("Batching is enabled, but batch duration is not positive");
        } else {
            return Flux.from(publisher).bufferTimeout(maxSize, maxDuration);
        }
    }
}
