package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Consumer;
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

    public static <T> AloMono<T> wrap(Publisher<Alo<T>> publisher) {
        return new AloMono<>(Mono.from(publisher));
    }

    public Mono<Alo<T>> unwrap() {
        return wrapped;
    }

    public AloMono<T> doFirst(Runnable onFirst) {
        return new AloMono<>(wrapped.doFirst(onFirst));
    }

    public AloMono<T> doOnNext(Consumer<? super T> onNext) {
        return new AloMono<>(wrapped.doOnNext(alo -> onNext.accept(alo.get())));
    }

    public AloMono<T> doOnError(Consumer<? super Throwable> onError) {
        return new AloMono<>(wrapped.doOnError(onError));
    }

    public AloMono<T> filter(Predicate<? super T> predicate) {
        return new AloMono<>(wrapped.filter(AloOps.wrapFilter(alo -> alo.filter(predicate, Alo::acknowledge))));
    }

    public <V> AloMono<V> map(Function<? super T, ? extends V> mapper) {
        return new AloMono<>(wrapped.map(AloOps.wrapMapper(alo -> alo.map(mapper))));
    }

    public <V> AloMono<V> flatMap(Function<? super T, Mono<V>> mapper) {
        return new AloMono<>(wrapped.flatMap(AloOps.wrapMapper(alo -> Mono.from(alo.publish(mapper)))));
    }

    public <R, C extends Collection<R>> AloFlux<R> flatMapCollection(Function<? super T, ? extends C> mapper) {
        return new AloFlux<>(wrapped.flatMapIterable(AloOps.wrapMapper(alo -> alo.mapToMany(mapper, Alo::acknowledge))));
    }

    public <V> AloFlux<V> flatMapMany(Function<? super T, ? extends Publisher<V>> mapper) {
        return new AloFlux<>(wrapped.flatMapMany(alo -> alo.publish(mapper)));
    }

    public AloMono<T> delayUntil(Function<? super T, ? extends Publisher<?>> triggerProvider) {
        return new AloMono<>(wrapped.delayUntil(alo -> triggerProvider.apply(alo.get())));
    }

    public <V> Flux<V> transformToMono(Function<? super AloMono<T>, ? extends Publisher<V>> transformer) {
        return Flux.from(transformer.apply(this));
    }

    public <P> P as(Function<? super AloMono<T>, P> transformer) {
        return transformer.apply(this);
    }

    public Disposable subscribe() {
        return subscribe(Alo::acknowledge);
    }

    public Disposable subscribe(Consumer<? super Alo<? super T>> consumer) {
        return wrapped.subscribe(consumer);
    }

    public Disposable subscribe(Consumer<? super Alo<? super T>> consumer, Consumer<? super Throwable> errorConsumer) {
        return wrapped.subscribe(consumer, errorConsumer);
    }

    @Override
    public void subscribe(Subscriber<? super Alo<T>> subscriber) {
        wrapped.subscribe(subscriber);
    }

    public <E extends Subscriber<? super Alo<T>>> E subscribeWith(E subscriber) {
        return wrapped.subscribeWith(subscriber);
    }
}
