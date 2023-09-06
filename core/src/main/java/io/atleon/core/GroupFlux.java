package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A wrapped {@link Flux} of grouped {@link AloFlux} sequences. Exposes convenience methods
 * (prefixed with "inner") to transform the emitted inner grouped fluxes. By convention, in order
 * to do useful operations on the underlying sequence(s) (like subscribe to them), any instance
 * must be converted (back) to an {@link AloFlux} through any of the "flatMapAlo" methods.
 *
 * @param <K> the type of groups emitted by this {@link GroupFlux}
 * @param <T> the type of {@link AloFlux} emitted as each group
 */
public class GroupFlux<K, T> {

    private final Flux<AloGroupedFlux<K, T>> wrapped;

    private final int cardinality;

    GroupFlux(Flux<AloGroupedFlux<K, T>> wrapped, int cardinality) {
        this.wrapped = wrapped;
        this.cardinality = cardinality;
    }

    static <K, T> GroupFlux<K, T> create(Flux<AloGroupedFlux<K, T>> flux, int cardinality) {
        return new GroupFlux<>(flux, cardinality);
    }

    /**
     * Return the underlying Flux backing this GroupFlux
     */
    public Flux<AloGroupedFlux<K, T>> unwrap() {
        return wrapped;
    }

    /**
     * Convenience method for applying {@link AloFlux#doOnNext(Consumer)} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, T> innerDoOnNext(Consumer<? super T> onNext) {
        return map(group -> group.doOnNext(onNext));
    }

    /**
     * Convenience method for applying {@link AloFlux#filter(Predicate)} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, T> innerFilter(Predicate<? super T> predicate) {
        return map(group -> group.filter(predicate));
    }

    /**
     * Convenience method for applying {@link AloFlux#map(Function)} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public <V> GroupFlux<K, V> innerMap(Function<? super T, ? extends V> mapper) {
        return map(group -> group.map(mapper));
    }

    /**
     * Convenience method for applying {@link AloFlux#mapNotNull(Function)} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public <V> GroupFlux<K, V> innerMapNotNull(Function<? super T, ? extends V> mapper) {
        return map(group -> group.mapNotNull(mapper));
    }

    /**
     * Convenience method for applying {@link AloFlux#mapPresent(Function)}} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public <V> GroupFlux<K, V> innerMapPresent(Function<? super T, Optional<? extends V>> mapper) {
        return map(group -> group.mapPresent(mapper));
    }

    /**
     * Convenience method for applying {@link AloFlux#consume(Consumer)}} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, Void> innerConsume(Consumer<? super T> consumer) {
        return map(group -> group.consume(consumer));
    }

    /**
     * Convenience method for applying {@link AloFlux#concatMap(Function)} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public <V> GroupFlux<K, V> innerConcatMap(Function<? super T, ? extends Publisher<V>> mapper) {
        return map(group -> group.concatMap(mapper));
    }

    /**
     * Convenience method for applying {@link AloFlux#concatMap(Function, int)} to each inner
     * grouped sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public <V> GroupFlux<K, V> innerConcatMap(Function<? super T, ? extends Publisher<V>> mapper, int prefetch) {
        return map(group -> group.concatMap(mapper, prefetch));
    }

    /**
     * Convenience method for applying {@link AloFlux#flatMapIterable(Function)} to each inner
     * grouped sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public <V> GroupFlux<K, V> innerFlatMapIterable(Function<? super T, ? extends Iterable<? extends V>> mapper) {
        return map(group -> group.flatMapIterable(mapper));
    }

    /**
     * Convenience method for applying {@link AloFlux#bufferTimeout(int, Duration)} to each inner
     * grouped sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, List<T>> innerBufferTimeout(int maxSize, Duration maxTime) {
        return map(group -> group.bufferTimeout(maxSize, maxTime));
    }

    /**
     * Convenience method for applying {@link AloFlux#bufferTimeout(int, Duration, Scheduler)} to
     * each inner grouped sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, List<T>> innerBufferTimeout(int maxSize, Duration maxTime, Scheduler scheduler) {
        return map(group -> group.bufferTimeout(maxSize, maxTime, scheduler));
    }

    /**
     * Convenience method for applying {@link AloFlux#publishOn(Scheduler)} to each inner grouped
     * sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, T> innerPublishOn(Scheduler scheduler) {
        return map(group -> group.publishOn(scheduler));
    }

    /**
     * Convenience method for applying {@link AloFlux#publishOn(Scheduler, int)} to each inner
     * grouped sequence.
     *
     * @return a transformed {@link GroupFlux}
     */
    public GroupFlux<K, T> innerPublishOn(Scheduler scheduler, int prefetch) {
        return map(group -> group.publishOn(scheduler, prefetch));
    }

    /**
     * @deprecated Use {{@link #map(Function)}} instead
     */
    @Deprecated
    public <V> GroupFlux<K, V> mapExtended(Function<? super AloGroupedFlux<K, T>, ? extends Publisher<Alo<V>>> mapper) {
        return map(mapper);
    }

    /**
     * Transform the items emitted by this {@link GroupFlux} by applying a synchronous function
     * to each item.
     *
     * @param mapper the synchronous transforming {@link Function}
     * @param <V>    the transformed type
     * @return a transformed {@link GroupFlux}
     * @see Flux#map(Function)
     */
    public <V> GroupFlux<K, V> map(Function<? super AloGroupedFlux<K, T>, ? extends Publisher<Alo<V>>> mapper) {
        return new GroupFlux<>(wrapped.map(group -> group.transformGrouped(mapper)), cardinality);
    }

    /**
     * Flatten each inner grouped Publisher in to a single {@link AloFlux}, allowing values to
     * interleave
     *
     * @return a new {@link AloFlux} of the merged results
     */
    public AloFlux<T> flatMapAlo() {
        return flatMapAlo(Function.identity());
    }

    /**
     * Transform the elements emitted by this {@link GroupFlux} asynchronously into Publishers of
     * {@link Alo}, then flatten these inner Publishers in to a single {@link AloFlux}, which
     * allows them to interleave
     *
     * @param mapper {@link Function} to transform each emission into {@link Publisher} of {@link Alo}
     * @param <V>    the type referenced by each resulting {@link Alo}
     * @return a new {@link AloFlux} of the merged results
     * @see Flux#flatMap(Function)
     */
    public <V> AloFlux<V> flatMapAlo(Function<? super AloGroupedFlux<K, T>, ? extends Publisher<Alo<V>>> mapper) {
        return wrapped.flatMap(mapper, cardinality).as(AloFlux::wrap);
    }
}
