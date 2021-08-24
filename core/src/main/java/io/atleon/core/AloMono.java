package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A wrapper around Project Reactor's {@link Mono} for Alo elements containing data of type T.
 * The goal of this wrapping is to make the handling of Alo more transparent such that clients
 * need only define pipelines in terms of T rather than Alo (of type T)
 *
 * @param <T> The type of elements contained in the Alo's of this reactive Publisher
 */
public class AloMono<T> implements Publisher<Alo<T>> {

    private final Mono<Alo<T>> wrapped;

    AloMono(Mono<Alo<T>> wrapped) {
        this.wrapped = wrapped;
    }

    public Mono<Alo<T>> unwrap() {
        return wrapped;
    }

    public AloMono<T> filter(Predicate<? super T> predicate) {
        return new AloMono<>(wrapped.filter(AloWrapping.wrapFilter(alo -> alo.filter(predicate, Alo::acknowledge))));
    }

    public <V> AloMono<V> map(Function<? super T, ? extends V> mapper) {
        return new AloMono<>(wrapped.map(AloWrapping.wrapMapper(alo -> alo.map(mapper))));
    }

    public <V> AloMono<V> flatMap(Function<? super T, Mono<V>> mapper) {
        return new AloMono<>(wrapped.flatMap(AloWrapping.wrapMapper(alo -> Mono.from(alo.publish(mapper)))));
    }

    public <R, C extends Collection<R>> AloFlux<R> flatMapCollection(Function<? super T, ? extends C> mapper) {
        return new AloFlux<>(wrapped.flatMapIterable(AloWrapping.wrapMapper(alo -> alo.mapToMany(mapper, Alo::acknowledge))));
    }

    public <V> AloFlux<V> flatMapMany(Function<? super T, ? extends Publisher<V>> mapper) {
        return new AloFlux<>(wrapped.flatMapMany(alo -> alo.publish(mapper)));
    }

    @Override
    public void subscribe(Subscriber<? super Alo<T>> subscriber) {
        wrapped.subscribe(subscriber);
    }
}
