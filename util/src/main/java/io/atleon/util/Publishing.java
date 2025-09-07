package io.atleon.util;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

import java.util.Collection;

/**
 * Utility functions for working with low-level {@link Publisher Publishers}.
 */
public final class Publishing {

    private Publishing() {

    }

    /**
     * Caches the first successful signal from subscribing to the provided {@link Mono}
     */
    public static <T> Mono<T> cacheSuccess(Mono<T> mono) {
        return mono.materialize().cacheInvalidateIf(Signal::hasError).dematerialize();
    }

    /**
     * "Greedily" merges all provided {@link Publisher Publishers} using a merge method that sets
     * the concurrency level equal to the number of Publishers to merge, returning a single
     * {@link Flux}.
     */
    public static <T> Flux<T> mergeGreedily(Collection<? extends Publisher<? extends T>> sources) {
        // Use merge method that takes explicit concurrency so that all provided publishers are
        // immediately (i.e. "greedily") subscribed.
        return Flux.merge(Flux.fromIterable(sources), Math.max(1, sources.size()));
    }
}
