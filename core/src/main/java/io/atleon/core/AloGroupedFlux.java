package io.atleon.core;

import reactor.core.publisher.GroupedFlux;

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

    AloGroupedFlux(GroupedFlux<? extends K, Alo<T>> groupedFlux) {
        super(groupedFlux);
        this.key = groupedFlux.key();
    }

    public K key() {
        return key;
    }
}