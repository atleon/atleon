package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

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
     * Create an AloFlux that emits the provided elements and then completes.
     */
    @SafeVarargs
    public static <T> AloFlux<T> just(Alo<T>... alos) {
        return wrap(Flux.just(alos));
    }

    /**
     * Convenience function for creating a wrapping {@link Function} based on a mapping of some raw
     * type into an {@link Alo} container. Useful for fluent passage to {@link Flux#as(Function)}.
     */
    public static <T, V> Function<Flux<T>, AloFlux<V>> wrapper(Function<? super T, ? extends Alo<V>> aloWrapper) {
        return flux -> wrap(flux.map(aloWrapper));
    }

    /**
     * Wraps a Publisher of Alo elements as an AloFlux
     */
    public static <T> AloFlux<T> wrap(Publisher<? extends Alo<T>> publisher) {
        return publisher instanceof AloFlux ? AloFlux.class.cast(publisher) : new AloFlux<>(Flux.from(publisher));
    }

    /**
     * Converts a Publisher of Alo elements to a Flux. Has the benefit of checking if the Publisher
     * is already an AloFlux such that no unnecessary wrapping occurs since an AloFlux can be
     * unwrapped
     */
    public static <T> Flux<Alo<T>> toFlux(Publisher<? extends Alo<T>> publisher) {
        return publisher instanceof AloFlux ? AloFlux.class.cast(publisher).unwrap() : Flux.from(publisher);
    }

    /**
     * Return the underlying Flux backing this AloFlux
     */
    public Flux<Alo<T>> unwrap() {
        return wrapped;
    }

    /**
     * @see Flux#doFirst(Runnable)
     */
    public AloFlux<T> doFirst(Runnable onFirst) {
        return new AloFlux<>(wrapped.doFirst(onFirst));
    }

    /**
     * @see Flux#doOnCancel(Runnable)
     */
    public AloFlux<T> doOnCancel(Runnable onCancel) {
        return new AloFlux<>(wrapped.doOnCancel(onCancel));
    }

    /**
     * @see Flux#doOnNext(Consumer)
     */
    public AloFlux<T> doOnNext(Consumer<? super T> onNext) {
        return new AloFlux<>(wrapped.doOnNext(alo -> alo.runInContext(() -> onNext.accept(alo.get()))));
    }

    /**
     * @see Flux#doOnError(Consumer)
     */
    public AloFlux<T> doOnError(Consumer<? super Throwable> onError) {
        return new AloFlux<>(wrapped.doOnError(onError));
    }

    /**
     * Similar to {@link #doFinally(Consumer)}, and provides the convenience being able to pass
     * multiple callbacks to be executed in provided order.
     *
     * @param onFinally The first callback to execute after stream termination
     * @param andThens  Subsequent callbacks to be executed in provided order
     * @return a transformed {@link AloFlux}
     */
    @SafeVarargs
    public final AloFlux<T> doAllFinally(Consumer<SignalType> onFinally, Consumer<SignalType>... andThens) {
        return doFinally(Stream.of(andThens).reduce(onFinally, Consumer::andThen));
    }

    /**
     * @see Flux#doFinally(Consumer)
     */
    public final AloFlux<T> doFinally(Consumer<SignalType> onFinally) {
        return new AloFlux<>(wrapped.doFinally(onFinally));
    }

    /**
     * @see Flux#doOnDiscard(Class, Consumer)
     */
    public <R> AloFlux<T> doOnDiscard(Class<R> type, Consumer<? super R> hook) {
        return new AloFlux<>(wrapped.contextWrite(DiscardHook.newContextModifier(type, hook)));
    }

    /**
     * Attach side-effect behavior to the positive acknowledgement of underlying emitted
     * {@link Alo} elements, passing the contained data which is/was emitted at this point in the
     * stream. Note that this behavior will be invoked <i>before</i> invoking the "native"
     * acknowledger.
     *
     * @param onAloAcknowledge The side-effect to invoke upon downstream positive acknowledgement
     * @return a transformed {@link AloFlux}
     */
    public AloFlux<T> doOnAloAcknowledge(Consumer<? super T> onAloAcknowledge) {
        return new AloFlux<>(wrapped.map(AloOps.acknowledgerDecorator(onAloAcknowledge)));
    }

    /**
     * @see Flux#filter(Predicate)
     */
    public AloFlux<T> filter(Predicate<? super T> predicate) {
        return new AloFlux<>(wrapped.handle(AloOps.filteringHandler(predicate, Alo::acknowledge)));
    }

    /**
     * Alias for {@code .map(clazz::cast)}
     */
    public <V> AloFlux<V> cast(Class<V> clazz) {
        return map(clazz::cast);
    }

    /**
     * Convenience method for applying a mapping and casting the result to a provided type.
     *
     * @param clazz  The type to cast mapped values to
     * @param mapper The mapping to apply
     * @param <V>    The type of emitted data elements
     * @return a transformed {@link AloFlux}
     */
    public <V> AloFlux<V> cast(Class<V> clazz, Function<? super T, ? super V> mapper) {
        return map(mapper.andThen(clazz::cast));
    }

    /**
     * See {@link Flux#ofType(Class)}
     */
    public <V> AloFlux<V> ofType(Class<V> clazz) {
        return new AloFlux<>(wrapped.handle(AloOps.typeFilteringHandler(clazz, Alo::acknowledge)));
    }

    /**
     * Convenience method for applying {@link AloDecorator} decoration to underlying emitted
     * {@link Alo} elements.
     */
    public AloFlux<T> decorateAlo(AloDecorator<T> decorator) {
        return new AloFlux<>(wrapped.map(decorator::decorate));
    }

    /**
     * @see Flux#map(Function)
     */
    public <V> AloFlux<V> map(Function<? super T, ? extends V> mapper) {
        return new AloFlux<>(wrapped.handle(AloOps.mappingHandler(mapper)));
    }

    /**
     * Transform the items emitted by this {@link AloFlux} by applying a synchronous function to
     * each item, which may produce {@code null} values. In that case, no value is emitted. This
     * method delegates to {@link AloFlux#mapPresent(Function)} by wrapping result values as
     * {@link Optional} and only emitting present values. This effectively results in two
     * {@link Alo#map(Function)} invocations for non-null values.
     *
     * @param mapper the synchronous transforming {@link Function}
     * @param <V>    the transformed type
     * @return a transformed {@link AloFlux}
     */
    public <V> AloFlux<V> mapNotNull(Function<? super T, ? extends V> mapper) {
        return mapPresent(mapper.andThen(Optional::ofNullable));
    }

    /**
     * Transform the items emitted by this {@link AloFlux} by applying a synchronous function to
     * each item that results in Optional values. When the result values are empty, no value is
     * emitted. This effectively results in two  {@link Alo#map(Function)} invocations for present
     * values.
     *
     * @param mapper the synchronous transforming {@link Function}
     * @param <V>    the transformed type
     * @return a transformed {@link AloFlux}
     */
    public <V> AloFlux<V> mapPresent(Function<? super T, Optional<? extends V>> mapper) {
        return new AloFlux<>(wrapped.handle(AloOps.mappingPresentHandler(mapper, Alo::acknowledge)));
    }

    /**
     * Apply a terminal consumption to the pipeline, acknowledging the corresponding {@link Alo}
     * upon successful consumer invocation. Resulting AloFlux will emit no values.
     *
     * @param consumer Terminal consumption to be applied to each {@link Alo}'s data item
     * @return A new AloFlux that emits no values
     */
    public AloFlux<Void> consume(Consumer<? super T> consumer) {
        return new AloFlux<>(wrapped.handle(AloOps.consumingHandler(consumer, Alo::acknowledge)));
    }

    /**
     * @see Flux#concatMap(Function)
     */
    public <V> AloFlux<V> concatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return wrapped.<Alo<Publisher<V>>>handle(AloOps.mappingHandler(mapper))
                .concatMap(AcknowledgingPublisher::fromAloPublisher)
                .as(AloFlux::new);
    }

    /**
     * @see Flux#concatMap(Function, int)
     */
    public <V> AloFlux<V> concatMap(Function<? super T, ? extends Publisher<V>> mapper, int prefetch) {
        return wrapped.<Alo<Publisher<V>>>handle(AloOps.mappingHandler(mapper))
                .concatMap(AcknowledgingPublisher::fromAloPublisher, prefetch)
                .as(AloFlux::new);
    }

    /**
     * @deprecated Use {@link AloFlux#flatMapIterable(Function)} instead
     */
    @Deprecated
    public <R> AloFlux<R> flatMapCollection(Function<? super T, ? extends Collection<? extends R>> mapper) {
        return flatMapIterable(mapper);
    }

    /**
     * @see Flux#flatMapIterable(Function)
     */
    public <R> AloFlux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
        return flatMap(mapper.andThen(Flux::fromIterable));
    }

    /**
     * @see Flux#flatMap(Function)
     */
    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return wrapped.<Alo<Publisher<V>>>handle(AloOps.mappingHandler(mapper))
                .flatMap(AcknowledgingPublisher::fromAloPublisher)
                .as(AloFlux::new);
    }

    /**
     * @see Flux#flatMap(Function, int)
     */
    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper, int concurrency) {
        return wrapped.<Alo<Publisher<V>>>handle(AloOps.mappingHandler(mapper))
                .flatMap(AcknowledgingPublisher::fromAloPublisher, concurrency)
                .as(AloFlux::new);
    }

    /**
     * @see Flux#flatMap(Function, int, int)
     */
    public <V> AloFlux<V> flatMap(Function<? super T, ? extends Publisher<V>> mapper, int concurrency, int prefetch) {
        return wrapped.<Alo<Publisher<V>>>handle(AloOps.mappingHandler(mapper))
                .flatMap(AcknowledgingPublisher::fromAloPublisher, concurrency, prefetch)
                .as(AloFlux::new);
    }

    /**
     * @see Flux#bufferTimeout(int, Duration)
     */
    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime) {
        return bufferTimeout(maxSize, maxTime, Schedulers.parallel());
    }

    /**
     * @see Flux#bufferTimeout(int, Duration, Scheduler)
     */
    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime, Scheduler scheduler) {
        return bufferTimeout(maxSize, maxTime, scheduler, false);
    }

    /**
     * @see Flux#bufferTimeout(int, Duration, boolean)
     */
    public AloFlux<List<T>> bufferTimeout(int maxSize, Duration maxTime, boolean fairBackpressure) {
        return bufferTimeout(maxSize, maxTime, Schedulers.parallel(), fairBackpressure);
    }

    /**
     * @see Flux#bufferTimeout(int, Duration, Scheduler, boolean)
     */
    public AloFlux<List<T>> bufferTimeout(
            int maxSize, Duration maxTime, Scheduler scheduler, boolean fairBackpressure) {
        return wrapped.bufferTimeout(maxSize, maxTime, scheduler, fairBackpressure)
                .map(AloOps::fanIn)
                .as(AloFlux::wrap);
    }

    /**
     * @see Flux#delayUntil(Function)
     */
    public AloFlux<T> delayUntil(Function<? super T, ? extends Publisher<?>> triggerProvider) {
        return new AloFlux<>(wrapped.delayUntil(alo -> alo.supplyInContext(() -> triggerProvider.apply(alo.get()))));
    }

    /**
     * @deprecated Use either {@link #deduplicate(Deduplication, DeduplicationConfig)} or other
     * simplified {@code deduplicate} APIs
     */
    @Deprecated
    public AloFlux<T> deduplicate(DeduplicationConfig config, Deduplication<T> deduplication) {
        return deduplicate(config, deduplication, Schedulers.boundedElastic());
    }

    /**
     * @deprecated Use either {@link #deduplicate(Deduplication, DeduplicationConfig, Scheduler)}
     * or other simplified {@code deduplicate} APIs
     */
    @Deprecated
    public AloFlux<T> deduplicate(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler scheduler) {
        return new AloFlux<>(wrapped.transform(DeduplicatingTransformer.alo(config, deduplication, scheduler)));
    }

    /**
     * Deduplicates emitted data items. For any given deduplication key, a reduced item is only
     * emitted after the deduplication Duration has elapsed since receiving the first item with
     * that key.
     *
     * @param deduplication Implementation of how to identify and deduplicate duplicate data items
     * @param timeout       The max duration in which to deduplicate items before emission
     * @return AloFlux of deduplicated items
     */
    public AloFlux<T> deduplicate(Deduplication<T> deduplication, Duration timeout) {
        return deduplicate(deduplication, timeout, Schedulers.boundedElastic());
    }

    /**
     * Deduplicates emitted data items. For any given deduplication key, a reduced item is only
     * emitted after the deduplication Duration has elapsed since receiving the first item with
     * that key.
     *
     * @param deduplication Implementation of how to identify and deduplicate duplicate data items
     * @param timeout       The max duration in which to deduplicate items before emission
     * @param scheduler     a time-capable Scheduler to run on
     * @return AloFlux of deduplicated items
     */
    public AloFlux<T> deduplicate(Deduplication<T> deduplication, Duration timeout, Scheduler scheduler) {
        return deduplicate(deduplication, new DeduplicationConfig(timeout), scheduler);
    }

    /**
     * Deduplicates emitted data items. For any given deduplication key, a reduced item is only
     * emitted after the deduplication Duration has elapsed since receiving the first item with
     * that key, or the max number of duplicate items have been received.
     *
     * @param deduplication Implementation of how to identify and deduplicate duplicate data items
     * @param config        Configuration of quantitative behaviors of Deduplication
     * @return AloFlux of deduplicated items
     */
    public AloFlux<T> deduplicate(Deduplication<T> deduplication, DeduplicationConfig config) {
        return deduplicate(deduplication, config, Schedulers.boundedElastic());
    }

    /**
     * Deduplicates emitted data items. For any given deduplication key, a reduced item is only
     * emitted after the deduplication Duration has elapsed since receiving the first item with
     * that key, or the max number of duplicate items have been received.
     *
     * @param deduplication Implementation of how to identify and deduplicate duplicate data items
     * @param config        Configuration of quantitative behaviors of Deduplication
     * @param scheduler     a time-capable Scheduler to run on
     * @return AloFlux of deduplicated items
     */
    public AloFlux<T> deduplicate(Deduplication<T> deduplication, DeduplicationConfig config, Scheduler scheduler) {
        return new AloFlux<>(wrapped.transform(DeduplicatingTransformer.alo(config, deduplication, scheduler)));
    }

    /**
     * Divide this sequence into dynamically created Flux (or groups) by hashing Number values
     * extracted from emitted data items. Note that there are guidelines and nuances to
     * appropriately consuming groups as explained under {@link Flux#groupBy(Function)}
     *
     * @param numberExtractor Function that extracts Numbers from data items for hashing
     * @param numGroups       How many groups to divide the source sequence in to
     * @return A Flux of grouped AloFluxes
     */
    public GroupFlux<Integer, T> groupByNumberHash(
            Function<? super T, ? extends Number> numberExtractor, int numGroups) {
        return groupBy(NumberHashGroupExtractor.composed(numberExtractor, numGroups), numGroups);
    }

    /**
     * Divide this sequence into dynamically created Flux (or groups) by hashing Number values
     * extracted from emitted data items. Note that there are guidelines and nuances to
     * appropriately consuming groups as explained under {@link Flux#groupBy(Function)}
     *
     * @param numberExtractor Function that extracts Numbers from data items for hashing
     * @param numGroups       How many groups to divide the source sequence in to
     * @return A Flux of grouped AloFluxes
     */
    public <V> GroupFlux<Integer, V> groupByNumberHash(
            Function<? super T, ? extends Number> numberExtractor, int numGroups, Function<? super T, V> valueMapper) {
        return groupBy(NumberHashGroupExtractor.composed(numberExtractor, numGroups), numGroups, valueMapper);
    }

    /**
     * Divide this sequence into dynamically created Flux (or groups) by hashing String values
     * extracted from emitted data items. Note that there are guidelines and nuances to
     * appropriately consuming groups as explained under {@link Flux#groupBy(Function)}
     *
     * @param stringExtractor Function that extracts Strings from data items for hashing
     * @param numGroups       How many groups to divide the source sequence in to
     * @return A Flux of grouped AloFluxes
     */
    public GroupFlux<Integer, T> groupByStringHash(Function<? super T, String> stringExtractor, int numGroups) {
        return groupBy(StringHashGroupExtractor.composed(stringExtractor, numGroups), numGroups);
    }

    /**
     * Divide this sequence into dynamically created Flux (or groups) by hashing String values
     * extracted from emitted data items. Note that there are guidelines and nuances to
     * appropriately consuming groups as explained under {@link Flux#groupBy(Function)}
     *
     * @param stringExtractor Function that extracts Strings from data items for hashing
     * @param numGroups       How many groups to divide the source sequence in to
     * @return A Flux of grouped AloFluxes
     */
    public <V> GroupFlux<Integer, V> groupByStringHash(
            Function<? super T, String> stringExtractor, int numGroups, Function<? super T, V> valueMapper) {
        return groupBy(StringHashGroupExtractor.composed(stringExtractor, numGroups), numGroups, valueMapper);
    }

    /**
     * @see Flux#groupBy(Function)
     */
    public <K> GroupFlux<K, T> groupBy(Function<? super T, ? extends K> groupExtractor, int cardinality) {
        return wrapped.groupBy(alo -> groupExtractor.apply(alo.get()))
                .<AloGroupedFlux<K, T>>map(AloGroupedFlux::create)
                .as(flux -> GroupFlux.create(flux, cardinality));
    }

    /**
     * @see Flux#groupBy(Function)
     */
    public <K, V> GroupFlux<K, V> groupBy(
            Function<? super T, ? extends K> groupExtractor, int cardinality, Function<? super T, V> valueMapper) {
        BiConsumer<Alo<T>, SynchronousSink<Alo<V>>> aloValueMappingHandler = AloOps.mappingHandler(valueMapper);
        return wrapped.groupBy(alo -> groupExtractor.apply(alo.get()))
                .<AloGroupedFlux<K, V>>map(groupedFlux -> AloGroupedFlux.create(groupedFlux, aloValueMappingHandler))
                .as(flux -> GroupFlux.create(flux, cardinality));
    }

    /**
     * Apply grouping by round-robin emission on the provided number of "rails"
     *
     * @param cardinality the number of processing "rails"
     * @return a {@link GroupFlux} where keys are rail IDs
     */
    public GroupFlux<Integer, T> groupByRoundRobin(int cardinality) {
        return wrapped.parallel(cardinality)
                .groups()
                .map(AloGroupedFlux::create)
                .as(flux -> GroupFlux.create(flux, cardinality));
    }

    /**
     * Group {@link Alo} elements by a key derived from their data into serialized
     * {@link AloGroupedFlux} sub-streams, where each emitted group automatically completes once
     * every element ever routed to it has signaled processing completion via positive or negative
     * acknowledgement. If the same key is observed again after its prior group has auto-completed,
     * a fresh {@link AloGroupedFlux} is emitted downstream for that key.
     *
     * <p>In contrast to {@link #groupBy(Function, int)} (which relies on
     * {@link Flux#groupBy(Function)} and therefore retains every group for the lifetime of the
     * source), this operator leverages the acknowledgement signal carried by every {@link Alo} to
     * release per-group resources as soon as a group becomes quiescent. This makes it suitable for
     * streams with high or unbounded key cardinality.
     *
     * <p>The {@code concurrency} parameter controls how many groups can be concurrently consumed
     * by subsequent {@link GroupFlux#flatMapAlo} operations. It does <i>not</i> bound the number
     * of distinct keys observed at the source; The amount of elements requested from the source is
     * rather unbounded, and it is therefore necessary that downstream group operators do <i>not</i>
     * restrict request count in an absolute manner. In other words, downstream group operators
     * should either request infinite elements, or continue requesting elements as long as they are
     * emitted into a given group. Otherwise, this can result in dropped elements that will never
     * be processed/acknowledged, which can lead to stream hanging.
     *
     * <p>Upon upstream error or downstream cancellation, active groups are completed (rather than
     * errored) and no attempt is made to positively or negatively acknowledge in-flight elements;
     * the upstream source is responsible for any necessary re-emission. See
     * {@link AloGroupByWithAutoCompleteOperator} for the full set of usage assumptions this
     * operator makes.
     *
     * @param keyExtractor function used to derive a grouping key from each element's data
     * @param concurrency  the number of groups that may be concurrently consumed downstream
     * @param <K>          the type of key produced by {@code keyExtractor}
     * @return a {@link GroupFlux} of self-completing {@link AloGroupedFlux} sub-streams keyed by {@code K}
     */
    public <K> GroupFlux<K, T> groupByWithAutoComplete(Function<? super T, ? extends K> keyExtractor, int concurrency) {
        Grouping<T, K> grouping = Grouping.simple(keyExtractor);
        return new GroupFlux<>(new AloGroupByWithAutoCompleteOperator<>(wrapped, grouping), concurrency);
    }

    /**
     * @see Flux#publishOn(Scheduler)
     */
    public AloFlux<T> publishOn(Scheduler scheduler) {
        return new AloFlux<>(wrapped.publishOn(scheduler));
    }

    /**
     * @see Flux#publishOn(Scheduler, int)
     */
    public AloFlux<T> publishOn(Scheduler scheduler, int prefetch) {
        return new AloFlux<>(wrapped.publishOn(scheduler, prefetch));
    }

    /**
     * @see Flux#subscribeOn(Scheduler)
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
     * @deprecated The backing method for this call is being deprecated by Reactor. It can be
     * replaced with explicit usage of {@link AloFlux#tap(SignalListenerFactory)} and a
     * {@link SignalListenerFactory} from Reactor or Atleon (see atleon-micrometer), or using
     * auto-decoration (see Atleon documentation for details)
     */
    @Deprecated
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
     * @deprecated The backing method for this call is being deprecated by Reactor. It can be
     * replaced with explicit usage of {@link AloFlux#tap(SignalListenerFactory)} and a
     * {@link SignalListenerFactory} from Reactor or Atleon (see atleon-micrometer), or using
     * auto-decoration (see Atleon documentation for details)
     */
    @Deprecated
    public AloFlux<T> metrics(String name, Map<String, String> tags) {
        Flux<Alo<T>> toWrap = wrapped.name(name);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            toWrap = toWrap.tag(entry.getKey(), entry.getValue());
        }
        return new AloFlux<>(toWrap.metrics());
    }

    /**
     * @see Flux#tap(SignalListenerFactory)
     */
    public AloFlux<T> tap(SignalListenerFactory<Alo<T>, ?> signalListenerFactory) {
        return new AloFlux<>(wrapped.tap(signalListenerFactory));
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
     * @see AloFlux#resubscribeOnError(ResubscriptionConfig)
     */
    public AloFlux<T> resubscribeOnError(String name) {
        return resubscribeOnError(new ResubscriptionConfig(name));
    }

    /**
     * @see AloFlux#resubscribeOnError(ResubscriptionConfig)
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
     * @see AloFlux#limitPerSecond(RateLimitingConfig, Scheduler)
     * @deprecated Use {@link #limitThroughput(Publisher)}
     */
    @Deprecated
    public AloFlux<T> limitPerSecond(double limitPerSecond) {
        return limitPerSecond(new RateLimitingConfig(limitPerSecond));
    }

    /**
     * @see AloFlux#limitPerSecond(RateLimitingConfig, Scheduler)
     * @deprecated Use {@link #limitThroughput(Publisher, Scheduler)}
     */
    @Deprecated
    public AloFlux<T> limitPerSecond(double limitPerSecond, Scheduler scheduler) {
        return limitPerSecond(new RateLimitingConfig(limitPerSecond), scheduler);
    }

    /**
     * @see AloFlux#limitPerSecond(RateLimitingConfig, Scheduler)
     * @deprecated Use {@link #limitThroughput(Publisher, Scheduler, int)}
     */
    @Deprecated
    public AloFlux<T> limitPerSecond(RateLimitingConfig config) {
        return limitPerSecond(config, Schedulers.boundedElastic());
    }

    /**
     * Limits the rate at which items are emitted by this Publisher. Especially useful in cases
     * where processing requires interaction with resource-constrained I/O dependencies
     * @deprecated Use {@link #limitThroughput(Publisher, Scheduler, int)}
     */
    @Deprecated
    public AloFlux<T> limitPerSecond(RateLimitingConfig config, Scheduler scheduler) {
        double countPerSecond = config.getPermitsPerSecond();
        Mono<Rate> limits = countPerSecond > 0 ? Mono.just(Rate.perSecond((long) countPerSecond)) : Mono.empty();
        return limitThroughput(limits, scheduler, config.getPrefetch());
    }

    /**
     * @see AloFlux#limitThroughput(Publisher, Scheduler, int)
     */
    public AloFlux<T> limitThroughput(Publisher<Rate> limits) {
        return limitThroughput(it -> it.limits(limits), 0);
    }

    /**
     * @see AloFlux#limitThroughput(Publisher, Scheduler, int)
     */
    public AloFlux<T> limitThroughput(Publisher<Rate> limits, Scheduler scheduler) {
        return limitThroughput(it -> it.limits(limits).scheduler(scheduler), 0);
    }

    /**
     * Apply a dynamic limit on the throughput of emitted elements. Each emission from the provided
     * limit publisher will be used to adjust the allowed throughput. Note that providing an empty
     * limit publisher will result in a stream with permanently unlimited throughput. If/when the
     * limit publisher emits a rate of zero, the stream will cancel itself unless/until the limit
     * publisher subsequently emits a non-zero rate, at which point the stream will resubscribe.
     * The limit publisher may also emit an "unlimited" rate, which will effectively disable
     * throughput limiting unless/until the limit publisher emits a non-infinite rate. Note that
     * the upstream will not be subscribed to until the limits publisher emits (or completes).
     *
     * @param limits    Publisher of dynamic throughput limits to apply
     * @param scheduler Scheduler used to realize non-blocking delay on throughput-limited emission
     * @param prefetch  Number of elements to prefetch for throughput-delayed emission
     * @return A new {@link AloFlux} with dynamic throughput limiting applied
     */
    public AloFlux<T> limitThroughput(Publisher<Rate> limits, Scheduler scheduler, int prefetch) {
        return limitThroughput(it -> it.limits(limits).scheduler(scheduler), prefetch);
    }

    /**
     * Apply {@link AloFailureStrategy} processing to emitted elements that may be error containers
     * or otherwise indicate an error.
     *
     * @param isFailure      Evaluates if an emitted item represents a failure
     * @param errorExtractor Function used to convert failed item to throwable error
     * @return A new {@link AloFlux} with failure processing applied
     */
    public AloFlux<T> processFailure(
            Predicate<? super T> isFailure, Function<? super T, ? extends Throwable> errorExtractor) {
        return new AloFlux<>(wrapped.handle(AloOps.failureProcessingHandler(isFailure, errorExtractor)));
    }

    /**
     * If an onAloError operator has been used downstream, reverts to the default 'EMIT' mode where
     * errors are emitted as terminal events upstream. This can be used for easier scoping of the
     * {@link AloFailureStrategy} or to override the inherited strategy in a sub-stream (for
     * example in a flatMap).
     *
     * @return A new {@link AloFlux} that terminally emits Alo failure errors
     */
    public AloFlux<T> onAloErrorEmit() {
        return new AloFlux<>(wrapped.contextWrite(Context.of(AloFailureStrategy.class, AloFailureStrategy.emit())));
    }

    /**
     * Emit failures on Alo elements as terminal errors in <strong>upstream</strong> operators
     * where the error does <strong>not</strong> match the provided predicate. When the predicate
     * <strong>is</strong> matched, recovery is accomplished by acknowledging the incriminating
     * Alo element and allowing the pipeline to continue.
     *
     * @param errorPredicate A {@link Predicate} used to filter which errors are ignored
     * @return A new {@link AloFlux} that terminally emits Alo failure errors unless filtered
     */
    public AloFlux<T> onAloErrorEmitUnless(Predicate<? super Throwable> errorPredicate) {
        return onAloErrorEmitUnless((__, error) -> errorPredicate.test(error));
    }

    /**
     * Emit failures on Alo elements as terminal errors in <strong>upstream</strong> operators
     * where the error does <strong>not</strong> match the provided bi-predicate which accepts the
     * value that triggered the error and the error itself. When the bi-predicate <strong>is</strong>
     * matched, recovery is accomplished by acknowledging the incriminating Alo element and
     * allowing the pipeline to continue.
     *
     * @param errorPredicate A {@link BiPredicate} used to filter which errors are ignored
     * @return A new {@link AloFlux} that terminally emits Alo failure errors unless filtered
     */
    public AloFlux<T> onAloErrorEmitUnless(BiPredicate<Object, ? super Throwable> errorPredicate) {
        AloFailureStrategy aloFailureStrategy = AloFailureStrategy.emit(errorPredicate.negate());
        return new AloFlux<>(wrapped.contextWrite(Context.of(AloFailureStrategy.class, aloFailureStrategy)));
    }

    /**
     * Delegates Alo failure handling to pipeline operators in <strong>upstream</strong> operators.
     * <p>
     * In the case of synchronous mapping operations, error delegation results in Alo elements
     * being negatively acknowledged. In the case of emitting elements that may contain errors
     * (like {@link SenderResult}), handling errors is delegated to downstream Subscribers.
     *
     * @return A new {@link AloFlux} that delegates failure handling to {@link Alo} or Subscribers
     */
    public AloFlux<T> onAloErrorDelegate() {
        return new AloFlux<>(wrapped.contextWrite(Context.of(AloFailureStrategy.class, AloFailureStrategy.delegate())));
    }

    /**
     * Delegates Alo failure handling to pipeline operators in <strong>upstream</strong> operators
     * where the error does <strong>not</strong> match the provided predicate. When the predicate
     * <strong>is</strong> matched, recovery is accomplished by acknowledging the incriminating Alo
     * element and allowing the pipeline to continue.
     * <p>
     * In the case of synchronous mapping operations, error delegation results in Alo elements
     * being negatively acknowledged. In the case of emitting elements that may contain errors
     * (like {@link SenderResult}), handling errors is delegated to downstream Subscribers.
     *
     * @param errorPredicate A {@link Predicate} used to filter which errors are ignored
     * @return A new {@link AloFlux} that delegates failure handling unless filtered
     */
    public AloFlux<T> onAloErrorDelegateUnless(Predicate<? super Throwable> errorPredicate) {
        return onAloErrorDelegateUnless((__, error) -> errorPredicate.test(error));
    }

    /**
     * Delegates Alo failure handling to pipeline operators in <strong>upstream</strong> operators
     * where the error does <strong>not</strong> match the provided bi-predicate (which accepts the
     * value that triggered the error and the error itself). When the predicate <strong>is</strong>
     * matched, recovery is accomplished by acknowledging the incriminating Alo element and
     * allowing the pipeline to continue.
     * <p>
     * In the case of synchronous mapping operations, error delegation results in Alo elements
     * being negatively acknowledged. In the case of emitting elements that may contain errors
     * (like {@link SenderResult}), handling errors is delegated to downstream Subscribers.
     *
     * @param errorPredicate A {@link BiPredicate} used to filter which errors are ignored
     * @return A new {@link AloFlux} that delegates failure handling unless filtered
     */
    public AloFlux<T> onAloErrorDelegateUnless(BiPredicate<Object, ? super Throwable> errorPredicate) {
        AloFailureStrategy aloFailureStrategy = AloFailureStrategy.delegate(errorPredicate.negate());
        return new AloFlux<>(wrapped.contextWrite(Context.of(AloFailureStrategy.class, aloFailureStrategy)));
    }

    /**
     * Adds a delegator for negative acknowledgement to {@link Alo} elements in this pipeline. Upon
     * downstream negative acknowledgement, the provided delegator will be invoked to produce a
     * {@link Publisher}. After subscribing to that error, successful completion of that publisher
     * implies that handling the error was delegated successfully, and the originating Alo can be
     * acknowledged. On the other hand, if the delegator publisher emits an error, that error will
     * be used to negatively acknowledge the associated Alo, with the original error added as a
     * suppressed exception (when possible).
     * <p>
     * Multiple delegators may be added to a pipeline. In this case, delegators are invoked in the
     * <i>reverse</i> order that they are applied in the pipeline.
     * <p>
     * In order for a delegator to be used, the Alo error mode must be set to "delegator" downstream
     * of this operator. For more information, see {@link #onAloErrorDelegate()}.
     *
     * @param delegator A {@link BiFunction} invoked when a downstream error occurs
     * @return A new {@link AloFlux}
     */
    public AloFlux<T> addAloErrorDelegation(
            BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegator) {
        return new AloFlux<>(new AloErrorDelegatingOperator<>(wrapped, delegator));
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
        return subscribeWith(new WarningAloSubscriber<>());
    }

    /**
     * @see Flux#subscribe(Consumer)
     */
    public Disposable subscribe(Consumer<? super Alo<T>> consumer) {
        return wrapped.subscribe(consumer);
    }

    /**
     * @see Flux#subscribe(Consumer, Consumer)
     */
    public Disposable subscribe(Consumer<? super Alo<T>> consumer, Consumer<? super Throwable> errorConsumer) {
        return wrapped.subscribe(consumer, errorConsumer);
    }

    /**
     * @see Publisher#subscribe(Subscriber)
     */
    @Override
    public void subscribe(Subscriber<? super Alo<T>> subscriber) {
        wrapped.subscribe(subscriber);
    }

    /**
     * @see Flux#subscribeWith(Subscriber)
     */
    public <E extends Subscriber<? super Alo<T>>> E subscribeWith(E subscriber) {
        return wrapped.subscribeWith(subscriber);
    }

    private AloFlux<T> limitThroughput(
            UnaryOperator<ThroughputLimitingTransformer.Builder<Alo<T>, Alo<T>>> configurator, int prefetch) {
        ThroughputLimitingTransformer<Alo<T>, Alo<T>> transformer = configurator
                .apply(ThroughputLimitingTransformer.concatMap(prefetch))
                .build();
        return wrap(wrapped.transform(transformer));
    }
}
