package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
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

    /**
     * Wraps a Publisher of Alo elements as an AloFlux
     */
    public static <T> AloFlux<T> wrap(Publisher<Alo<T>> publisher) {
        return publisher instanceof AloFlux ? AloFlux.class.cast(publisher) : new AloFlux<>(Flux.from(publisher));
    }

    /**
     * Converts a Publisher of Alo elements to a Flux. Has the benefit of checking if the Publisher
     * is already an AloFlux such that no unnecessary wrapping occurs since an AloFlux can be
     * unwrapped
     */
    public static <T> Flux<Alo<T>> toFlux(Publisher<Alo<T>> publisher) {
        return publisher instanceof AloFlux ? AloFlux.class.cast(publisher).unwrap() : Flux.from(publisher);
    }

    /**
     * Return the underlying Flux backing this AloFlux
     */
    public Flux<Alo<T>> unwrap() {
        return wrapped;
    }

    /**
     * See {@link Flux#doFirst(Runnable)}
     */
    public AloFlux<T> doFirst(Runnable onFirst) {
        return new AloFlux<>(wrapped.doFirst(onFirst));
    }

    /**
     * See {@link Flux#doOnNext(Consumer)}
     */
    public AloFlux<T> doOnNext(Consumer<? super T> onNext) {
        return new AloFlux<>(wrapped.doOnNext(alo -> onNext.accept(alo.get())));
    }

    /**
     * See {@link Flux#doOnError(Consumer)}
     */
    public AloFlux<T> doOnError(Consumer<? super Throwable> onError) {
        return new AloFlux<>(wrapped.doOnError(onError));
    }

    /**
     * See {@link Flux#filter(Predicate)}
     */
    public AloFlux<T> filter(Predicate<? super T> predicate) {
        return new AloFlux<>(wrapped.filter(AloOps.filtering(predicate, Alo::acknowledge)));
    }

    /**
     * See {@link Flux#map(Function)}
     */
    public <V> AloFlux<V> map(Function<? super T, ? extends V> mapper) {
        return new AloFlux<>(wrapped.map(AloOps.mapping(mapper)));
    }

    /**
     * See {@link Flux#concatMap(Function)}
     */
    public <V> AloFlux<V> concatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return new AloFlux<>(wrapped.concatMap(AloOps.publishing(mapper)));
    }

    /**
     * See {@link Flux#concatMap(Function, int)}
     */
    public <V> AloFlux<V> concatMap(Function<? super T, ? extends Publisher<V>> mapper, int prefetch) {
        return new AloFlux<>(wrapped.concatMap(AloOps.publishing(mapper), prefetch));
    }

    /**
     * See {@link Flux#flatMap(Function)}
     */
    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return new AloFlux<>(wrapped.flatMap(AloOps.publishing(mapper)));
    }

    /**
     * See {@link Flux#flatMap(Function, int)}
     */
    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper, int concurrency) {
        return new AloFlux<>(wrapped.flatMap(AloOps.publishing(mapper), concurrency));
    }

    /**
     * See {@link Flux#flatMap(Function, int, int)}
     */
    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper, int concurrency, int prefetch) {
        return new AloFlux<>(wrapped.flatMap(AloOps.publishing(mapper), concurrency, prefetch));
    }

    /**
     * Similar to (and backed by) {@link Flux#flatMapIterable(Function)}, map each emitted Alo data
     * item to a Collection of results, which are then flattened and emitted as Alo-wrapped
     * elements.
     *
     * @param mapper Function that maps each data item to a Collection of results
     */
    public <R, C extends Collection<R>> AloFlux<R> flatMapCollection(Function<? super T, ? extends C> mapper) {
        return new AloFlux<>(wrapped.flatMapIterable(AloOps.mappingToMany(mapper, Alo::acknowledge)));
    }

    /**
     * See {@link Flux#bufferTimeout(int, Duration)}
     */
    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime) {
        return bufferTimeout(maxSize, maxTime, Schedulers.parallel(), buffer -> buffer.get(0).propagator());
    }

    /**
     * See {@link Flux#bufferTimeout(int, Duration, Scheduler)}
     */
    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime, Scheduler scheduler) {
        return bufferTimeout(maxSize, maxTime, scheduler, buffer -> buffer.get(0).propagator());
    }

    /**
     * Same {@link AloFlux#bufferTimeout(int, Duration, Scheduler, Function)} with default
     * Scheduler of {@link Schedulers#parallel()}
     */
    public AloFlux<List<T>> bufferTimeout(
        int maxSize,
        Duration maxTime,
        Function<? super List<Alo<T>>, AloFactory<List<T>>> bufferToAloFactory) {
        return bufferTimeout(maxSize, maxTime, Schedulers.parallel(), bufferToAloFactory);
    }

    /**
     * With the same semantics as {@link Flux#bufferTimeout(int, Duration, Scheduler)}, this
     * operator causes items to be buffered in to Lists within bounded size and time. The one
     * nuance to this method is the provided Function which produces an {@link AloFactory} which is
     * used to create an implementation of Alo that wraps the buffered data items.
     *
     * @param maxSize – the max collected size
     * @param maxTime – the timeout enforcing the release of a partial buffer
     * @param scheduler – a time-capable Scheduler instance to run on
     * @param bufferToAloFactory - Function that provides an AloFactory to wrap list of items
     * @return
     */
    public AloFlux<List<T>> bufferTimeout(
        int maxSize,
        Duration maxTime,
        Scheduler scheduler,
        Function<? super List<Alo<T>>, AloFactory<List<T>>> bufferToAloFactory) {
        return wrapped.bufferTimeout(maxSize, maxTime, scheduler)
            .map(buffer -> AloFactory.invertList(buffer, bufferToAloFactory.apply(buffer)))
            .as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#delayUntil(Function)}
     */
    public AloFlux<T> delayUntil(Function<? super T, ? extends Publisher<?>> triggerProvider) {
        return new AloFlux<>(wrapped.delayUntil(alo -> triggerProvider.apply(alo.get())));
    }

    /**
     * Deduplicates emitted data items. Deduplicated items are emitted only after either the max
     * number of duplicate items are received or the deduplication Duration has elapsed since
     * receiving the first of any given item with a particular dedpulication key
     *
     * @param config Configuration of quantitative behaviors of Deduplication
     * @param deduplication Implementation of how to identify and deduplicate duplicate data items
     * @return AloFlux of deduplicated items
     */
    public AloFlux<T> deduplicate(DeduplicationConfig config, Deduplication<T> deduplication) {
        return new AloFlux<>(wrapped.transform(DeduplicatingTransformer.alo(config, deduplication)));
    }

    /**
     * Deduplicates emitted data items. Deduplicated items are emitted only after either the max
     * number of duplicate items are received or the deduplication Duration has elapsed since
     * receiving the first of any given item with a particular dedpulication key
     *
     * @param config Configuration of quantitative behaviors of Deduplication
     * @param deduplication Implementation of how to identify and deduplicate duplicate data items
     * @param scheduler a time-capable Scheduler to run on
     * @return AloFlux of deduplicated items
     */
    public AloFlux<T> deduplicate(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler scheduler) {
        return new AloFlux<>(wrapped.transform(DeduplicatingTransformer.alo(config, deduplication, scheduler)));
    }

    /**
     * See {@link Flux#reduce(BiFunction)}
     */
    public AloMono<T> reduce(BinaryOperator<T> reducer) {
        return new AloMono<>(wrapped.reduce(AloOps.reducing(reducer)));
    }

    /**
     * Divide this sequence into dynamically created Flux (or groups) by hashing String values
     * extracted from emitted data items. Note that there are guidelines and nuances to
     * appropriately consuming groups as explained under {@link Flux#groupBy(Function)}
     *
     * @param stringExtractor Function that extracts Strings from data items for hashing
     * @param numGroups How many groups to divide the source sequence in to
     * @return A Flux of grouped AloFluxes
     */
    public AloExtendedFlux<AloGroupedFlux<Integer, T>>
    groupByStringHash(Function<? super T, String> stringExtractor, int numGroups) {
        return groupBy(StringHashGroupExtractor.composed(stringExtractor, numGroups));
    }

    /**
     * Divide this sequence into dynamically created Flux (or groups) by hashing String values
     * extracted from emitted data items. Note that there are guidelines and nuances to
     * appropriately consuming groups as explained under {@link Flux#groupBy(Function)}
     *
     * @param stringExtractor Function that extracts Strings from data items for hashing
     * @param numGroups How many groups to divide the source sequence in to
     * @return A Flux of grouped AloFluxes
     */
    public <V> AloExtendedFlux<AloGroupedFlux<Integer, V>>
    groupByStringHash(Function<? super T, String> stringExtractor, int numGroups, Function<? super T, V> valueMapper) {
        return groupBy(StringHashGroupExtractor.composed(stringExtractor, numGroups), valueMapper);
    }

    /**
     * See {@link Flux#groupBy(Function)}
     */
    public <K> AloExtendedFlux<AloGroupedFlux<K, T>> groupBy(Function<? super T, ? extends K> groupExtractor) {
        return wrapped.groupBy(alo -> groupExtractor.apply(alo.get()))
            .<AloGroupedFlux<K, T>>map(AloGroupedFlux::new)
            .as(AloExtendedFlux::new);
    }

    /**
     * See {@link Flux#groupBy(Function)}
     */
    public <K, V> AloExtendedFlux<AloGroupedFlux<K, V>>
    groupBy(Function<? super T, ? extends K> groupExtractor, Function<? super T, V> valueMapper) {
        return wrapped.groupBy(alo -> groupExtractor.apply(alo.get()), AloOps.mapping(valueMapper))
            .<AloGroupedFlux<K, V>>map(AloGroupedFlux::new)
            .as(AloExtendedFlux::new);
    }

    /**
     * See {@link Flux#publishOn(Scheduler)}
     */
    public AloFlux<T> publishOn(Scheduler scheduler) {
        return new AloFlux<>(wrapped.publishOn(scheduler));
    }

    /**
     * See {@link Flux#publishOn(Scheduler, int)}
     */
    public AloFlux<T> publishOn(Scheduler scheduler, int prefetch) {
        return new AloFlux<>(wrapped.publishOn(scheduler, prefetch));
    }

    /**
     * See {@link Flux#subscribeOn(Scheduler)}
     */
    public AloFlux<T> subscribeOn(Scheduler scheduler) {
        return new AloFlux<>(wrapped.subscribeOn(scheduler));
    }

    /**
     * Names and activates metrics for this AloFlux. Tuples of tags can be provided to attach to
     * provided metrics
     *
     * @param name The name to be assigned to this sequence and used for Meter names
     * @param tags Tuples of tags used to create Meters
     */
    public AloFlux<T> metrics(String name, String... tags) {
        if (tags.length % 2 != 0) {
            throw new IllegalArgumentException("Tags must be key-value tuples");
        }
        Flux<Alo<T>> toWrap = wrapped.name(name);
        for (int i = 0; i < tags.length; i = i + 2) {
            toWrap = toWrap.tag(tags[i], tags[i + 1]);
        }
        return new AloFlux<>(toWrap.metrics());
    }

    /**
     * Names and activates metrics for this AloFlux. Provided tags are also used to create Meters
     *
     * @param name The name to be assigned to this sequence and used for Meter names
     * @param tags Tags used during metric identification and creation
     */
    public AloFlux<T> metrics(String name, Map<String, String> tags) {
        Flux<Alo<T>> toWrap = wrapped.name(name);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            toWrap = toWrap.tag(entry.getKey(), entry.getValue());
        }
        return new AloFlux<>(toWrap.metrics());
    }

    /**
     * Enforces activity on this sequence by emitting an error in to the sequence if a
     * configurable Duration passes between one signal (subscription, request, onNext) and the
     * receipt of any other signal. This is typically useful for detecting deadlocked streams and
     * when coupled with a downstream {@link AloFlux#resubscribeOnError(ResubscriptionConfig)} can
     * help remedy transient stream deadlock issues.
     */
    public AloFlux<T> enforceActivity(ActivityEnforcementConfig config) {
        return new AloFlux<>(wrapped.transform(new ActivityEnforcingTransformer<>(config)));
    }

    /**
     * See {@link AloFlux#resubscribeOnError(ResubscriptionConfig)}
     */
    public AloFlux<T> resubscribeOnError(String name) {
        return resubscribeOnError(new ResubscriptionConfig(name));
    }

    /**
     * See {@link AloFlux#resubscribeOnError(ResubscriptionConfig)}
     */
    public AloFlux<T> resubscribeOnError(String name, Duration delay) {
        return resubscribeOnError(new ResubscriptionConfig(name, delay));
    }

    /**
     * Upon occurrence of any given Error, resubscribe to the upstream with a configurable delay
     */
    public AloFlux<T> resubscribeOnError(ResubscriptionConfig config) {
        return new AloFlux<>(wrapped.transform(new ResubscribingTransformer<>(config)));
    }

    /**
     * See {@link AloFlux#limitPerSecond(RateLimitingConfig)}
     */
    public AloFlux<T> limitPerSecond(double limitPerSecond) {
        return limitPerSecond(new RateLimitingConfig(limitPerSecond));
    }

    /**
     * Limits the rate at which items are emitted by this Publisher. Especially useful in cases
     * where processing requires interaction with resource-contrained I/O dependencies
     */
    public AloFlux<T> limitPerSecond(RateLimitingConfig config) {
        return new AloFlux<>(wrapped.transform(new RateLimitingTransformer<>(config)));
    }

    /**
     * Transform this AloFlux to another AloFlux by mapping it to another Publisher of Alo. Note
     * that it is the responsibility of the transformation to assure that Alo elements from this
     * sequence have their acknowledgement propagated through the provided transformer
     */
    public <V> AloFlux<V> transform(Function<? super AloFlux<T>, ? extends Publisher<Alo<V>>> transformer) {
        return wrap(transformer.apply(this));
    }

    /**
     * Transform this AloFlux to a Flux by mapping it to a Publisher of any other type. Note that
     * it is the responsibility of the transformation to assure that Alo elements from this
     * sequence have their acknowledgement managed in the transformation process
     */
    public <V> Flux<V> transformToFlux(Function<? super AloFlux<T>, ? extends Publisher<V>> transformer) {
        return Flux.from(transformer.apply(this));
    }

    /**
     * Applies consumption of emitted Alo items (typically {@link Alo#acknowledge}) and retrieves
     * the data item
     *
     * @param aloConsumer The consumer to apply to Alo before retrieving the data item
     * @return {@link Flux} of retrieved data items
     */
    public Flux<T> consumeAloAndGet(Consumer<? super Alo<? super T>> aloConsumer) {
        return wrapped.map(alo -> {
            aloConsumer.accept(alo);
            return alo.get();
        });
    }

    /**
     * Transform this AloFlux in to a target type
     */
    public <P> P as(Function<? super AloFlux<T>, P> transformer) {
        return transformer.apply(this);
    }

    /**
     * Subscribe to this AloFlux by acknowledging all emitted items. Note that this should only be
     * used as a subscription method when it is not likely for the upstream to emit errors and is
     * not the case that data items contained in emitted Alo elements carry relevant errors
     */
    public Disposable subscribe() {
        return subscribe(Alo::acknowledge);
    }

    /**
     * See {@link Flux#subscribe(Consumer)}
     */
    public Disposable subscribe(Consumer<? super Alo<? super T>> consumer) {
        return wrapped.subscribe(consumer);
    }

    /**
     * See {@link Flux#subscribe(Consumer, Consumer)}
     */
    public Disposable subscribe(Consumer<? super Alo<? super T>> consumer, Consumer<? super Throwable> errorConsumer) {
        return wrapped.subscribe(consumer, errorConsumer);
    }

    /**
     * See {@link Publisher#subscribe(Subscriber)}
     */
    @Override
    public void subscribe(Subscriber<? super Alo<T>> subscriber) {
        wrapped.subscribe(subscriber);
    }

    /**
     * See {@link Flux#subscribeWith(Subscriber)}
     */
    public <E extends Subscriber<? super Alo<T>>> E subscribeWith(E subscriber) {
        return wrapped.subscribeWith(subscriber);
    }
}
