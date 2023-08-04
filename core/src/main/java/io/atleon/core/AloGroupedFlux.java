package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.SynchronousSink;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A wrapper around Project Reactor's {@link GroupedFlux} for keys of type K and for Alo elements
 * containing data of type T. The goal of this wrapping is to make the handling of Alo more
 * transparent such that clients need only define pipelines in terms of T versus Alo (of type T)
 *
 * @param <K> The type of key for this AloFlux is grouped to
 * @param <T> The type of elements contained in the Alo's of this reactive Publisher
 */
public class AloGroupedFlux<K, T> extends AloFlux<T> {

    private final K key;

    private AloGroupedFlux(Flux<Alo<T>> flux, K key) {
        super(flux);
        this.key = key;
    }

    static <K, T> AloGroupedFlux<K, T> create(GroupedFlux<? extends K, Alo<T>> groupedFlux) {
        return new AloGroupedFlux<>(groupedFlux, groupedFlux.key());
    }

    static <K, T, R> AloGroupedFlux<K, R>
    create(GroupedFlux<? extends K, Alo<T>> groupedFlux, BiConsumer<Alo<T>, SynchronousSink<Alo<R>>> mappingHandler) {
        return new AloGroupedFlux<>(groupedFlux.handle(mappingHandler), groupedFlux.key());
    }

    public <V> AloGroupedFlux<K, V>
    transformGrouped(Function<? super AloGroupedFlux<K, T>, ? extends Publisher<Alo<V>>> transformer) {
        return new AloGroupedFlux<>(AloFlux.toFlux(transformer.apply(this)), key);
    }

    public K key() {
        return key;
    }
}