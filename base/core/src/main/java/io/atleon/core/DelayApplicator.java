package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Interface through which per-element delays can be applied to a source, which is assumed to emit
 * either elements of the delayed type, or elements containing/emitting that type (i.e. a grouped
 * Flux of elements).
 */
@FunctionalInterface
interface DelayApplicator<T, V> {

    static <T> DelayApplicator<T, T> concatMap() {
        return Flux::concatMap;
    }

    static <T, V> DelayApplicator<T, V> innerConcatMap(
            Function<V, ? extends Publisher<T>> innerUnwrapper, BiFunction<V, ? super Flux<T>, V> innerRewrapper) {
        return (outer, delayer, prefetch) -> outer.map(originalInner -> {
            Flux<T> elements = Flux.from(innerUnwrapper.apply(originalInner));
            return innerRewrapper.apply(originalInner, elements.concatMap(delayer, prefetch));
        });
    }

    Flux<V> apply(Flux<V> source, Function<T, ? extends Publisher<T>> delayer, int prefetch);
}
