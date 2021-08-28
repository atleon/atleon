package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A wrapper around Project Reactor's {@link Flux} for Alo elements containing data of type T.
 * The goal of this wrapping is to make the handling of Alo more transparent such that clients
 * need only define pipelines in terms of T rather than Alo (of type T)
 *
 * @param <T> The type of elements contained in the Alo's of this reactive Publisher
 */
public class AloFlux<T> implements Publisher<Alo<T>> {

    private final Flux<Alo<T>> wrapped;

    AloFlux(Flux<Alo<T>> wrapped) {
        this.wrapped = Flux.from(wrapped);
    }

    public static <T> AloFlux<T> wrap(Publisher<Alo<T>> publisher) {
        return publisher instanceof AloFlux ? AloFlux.class.cast(publisher) : new AloFlux<>(Flux.from(publisher));
    }

    public static <T> Flux<Alo<T>> toFlux(Publisher<Alo<T>> publisher) {
        return publisher instanceof AloFlux ? AloFlux.class.cast(publisher).unwrap() : Flux.from(publisher);
    }

    public Flux<Alo<T>> unwrap() {
        return wrapped;
    }

    public AloFlux<T> doFirst(Runnable onFirst) {
        return new AloFlux<>(wrapped.doFirst(onFirst));
    }

    public AloFlux<T> doOnNext(Consumer<? super T> onNext) {
        return new AloFlux<>(wrapped.doOnNext(alo -> onNext.accept(alo.get())));
    }

    public AloFlux<T> doOnError(Consumer<? super Throwable> onError) {
        return new AloFlux<>(wrapped.doOnError(onError));
    }

    public AloFlux<T> filter(Predicate<? super T> predicate) {
        return new AloFlux<>(wrapped.filter(AloOps.wrapFilter(alo -> alo.filter(predicate, Alo::acknowledge))));
    }

    public <V> AloFlux<V> map(Function<? super T, ? extends V> mapper) {
        return new AloFlux<>(wrapped.map(AloOps.wrapMapper(alo -> alo.map(mapper))));
    }

    public <V> AloFlux<V> concatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return new AloFlux<>(wrapped.concatMap(AloOps.wrapMapper(alo -> alo.publish(mapper))));
    }

    public <V> AloFlux<V> concatMap(Function<? super T, ? extends Publisher<V>> mapper, int prefetch) {
        return new AloFlux<>(wrapped.concatMap(AloOps.wrapMapper(alo -> alo.publish(mapper)), prefetch));
    }

    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return new AloFlux<>(wrapped.flatMap(AloOps.wrapMapper(alo -> alo.publish(mapper))));
    }

    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper, int concurrency) {
        return new AloFlux<>(wrapped.flatMap(AloOps.wrapMapper(alo -> alo.publish(mapper)), concurrency));
    }

    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper, int concurrency, int prefetch) {
        return new AloFlux<>(wrapped.flatMap(AloOps.wrapMapper(alo -> alo.publish(mapper)), concurrency, prefetch));
    }

    public <R, C extends Collection<R>> AloFlux<R> flatMapCollection(Function<? super T, ? extends C> mapper) {
        return new AloFlux<>(wrapped.concatMapIterable(AloOps.wrapMapper(alo -> alo.mapToMany(mapper, Alo::acknowledge))));
    }

    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime) {
        return bufferTimeout(maxSize, maxTime, Schedulers.parallel(), buffer -> buffer.get(0)::propagate);
    }

    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime, Scheduler scheduler) {
        return bufferTimeout(maxSize, maxTime, scheduler, buffer -> buffer.get(0)::propagate);
    }

    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime, Function<? super List<Alo<T>>, AloFactory<List<T>>> bufferToAloFactory) {
        return bufferTimeout(maxSize, maxTime, Schedulers.parallel(), buffer -> buffer.get(0)::propagate);
    }

    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime, Scheduler scheduler, Function<? super List<Alo<T>>, AloFactory<List<T>>> bufferToAloFactory) {
        return wrapped.bufferTimeout(maxSize, maxTime, scheduler)
            .map(buffer -> AloFactory.invertList(buffer, bufferToAloFactory.apply(buffer)))
            .as(AloFlux::wrap);
    }

    public AloFlux<T> delayUntil(Function<? super T, ? extends Publisher<?>> triggerProvider) {
        return new AloFlux<>(wrapped.delayUntil(alo -> triggerProvider.apply(alo.get())));
    }

    public AloFlux<T> deduplicate(DeduplicationConfig config, Deduplication<T> deduplication) {
        return new AloFlux<>(wrapped.transform(DeduplicatingTransformer.alo(config, deduplication)));
    }

    public AloFlux<T> deduplicate(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler scheduler) {
        return new AloFlux<>(wrapped.transform(DeduplicatingTransformer.alo(config, deduplication, scheduler)));
    }

    public AloMono<T> reduce(BinaryOperator<T> reducer) {
        return new AloMono<>(wrapped.reduce(AloOps.wrapAggregator((alo1, alo2) -> alo1.reduce(reducer, alo2))));
    }

    public Flux<AloGroupedFlux<Integer, T>> groupByStringHash(Function<? super T, String> stringExtractor, int numGroups) {
        return groupBy(StringHashGroupExtractor.composed(stringExtractor, numGroups));
    }

    public <K> Flux<AloGroupedFlux<K, T>> groupBy(Function<? super T, ? extends K> groupExtractor) {
        return wrapped.groupBy(alo -> groupExtractor.apply(alo.get()))
            .map(AloGroupedFlux::new);
    }

    public AloFlux<T> enforceActivity(ActivityEnforcementConfig config) {
        return new AloFlux<>(wrapped.transformDeferred(new ActivityEnforcingTransformer<>(config)));
    }

    public AloFlux<T> resubscribeOnError(String name) {
        return resubscribeOnError(new ResubscriptionConfig(name));
    }

    public AloFlux<T> resubscribeOnError(String name, Duration duration) {
        return resubscribeOnError(new ResubscriptionConfig(name, duration));
    }

    public AloFlux<T> resubscribeOnError(ResubscriptionConfig config) {
        return new AloFlux<>(wrapped.transform(new ResubscribingTransformer<>(config)));
    }

    public AloFlux<T> limitPerSecond(double limitPerSecond) {
        return limitPerSecond(new RateLimitingConfig(limitPerSecond));
    }

    public AloFlux<T> limitPerSecond(RateLimitingConfig config) {
        return new AloFlux<>(wrapped.transform(new RateLimitingTransformer<>(config)));
    }

    public <V> AloFlux<V> transform(Function<? super AloFlux<T>, ? extends Publisher<Alo<V>>> transformer) {
        return wrap(transformer.apply(this));
    }

    public <V> Flux<V> transformToFlux(Function<? super AloFlux<T>, ? extends Publisher<V>> transformer) {
        return Flux.from(transformer.apply(this));
    }

    public Flux<T> consumeAloAndGet(Consumer<? super Alo<? super T>> aloConsumer) {
        return wrapped.map(alo -> {
            aloConsumer.accept(alo);
            return alo.get();
        });
    }

    public <P> P as(Function<? super AloFlux<T>, P> transformer) {
        return transformer.apply(this);
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
