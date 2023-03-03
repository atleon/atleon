package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An {@link Alo}-aware wrapper of a {@link Flux}. All methods delegate to wrapped Flux, and all
 * {@code *Alo} methods map to result types that allow them to be wrapped as {@link AloFlux} and
 * are wrapped as such.
 *
 * @deprecated All past operations resulting AloExtendedFlux now result in {@link GroupFlux}, which
 * should be used directly
 */
@Deprecated
public interface AloExtendedFlux<T> extends Publisher<T> {

    /**
     * @deprecated Create Publisher/Flux of Alo and use {@link AloFlux#wrap(Publisher)}
     */
    @Deprecated
    static <T> AloExtendedFlux<T> wrap(Publisher<? extends T> publisher) {
        return publisher instanceof GroupFlux
            ? GroupFlux.class.cast(publisher)
            : new GroupFlux<>(Flux.from(publisher), Integer.MAX_VALUE);
    }

    /**
     * @deprecated Use {@link AloExtendedFlux#map(Function)}
     */
    @Deprecated
    default <V> AloExtendedFlux<V> mapExtended(Function<? super T, ? extends V> mapper) {
        return map(mapper);
    }

    /**
     * @deprecated Reference {@link GroupFlux} and use {@link GroupFlux#flatMapAlo(Function)}
     */
    @Deprecated
    default <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper, int concurrency) {
        return unwrap().flatMap(mapper, concurrency).as(AloFlux::wrap);
    }

    /**
     * @deprecated Reference {@link GroupFlux} and use {@link GroupFlux#flatMapAlo(Function)}
     */
    @Deprecated
    default <V> AloFlux<V>
    flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper, int concurrency, int prefetch) {
        return unwrap().flatMap(mapper, concurrency, prefetch).as(AloFlux::wrap);
    }

    /**
     * @deprecated Reference {@link GroupFlux} and use {@link GroupFlux#flatMapAlo(Function)}
     */
    @Deprecated
    default <V> AloFlux<V> transformAlo(Function<? super AloExtendedFlux<T>, ? extends Publisher<Alo<V>>> transformer) {
        return AloFlux.wrap(transformer.apply(this));
    }

    /**
     * @deprecated It is not intended for users to subscribe directly to this Flux
     */
    @Deprecated
    default Disposable subscribe(Consumer<? super T> consumer) {
        return unwrap().subscribe(consumer);
    }

    /**
     * @deprecated It is not intended for users to subscribe directly to this Flux
     */
    @Deprecated
    default Disposable subscribe(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
        return unwrap().subscribe(consumer, errorConsumer);
    }

    /**
     * @deprecated It is not intended for users to subscribe directly to this Flux
     */
    @Override
    default void subscribe(Subscriber<? super T> s) {
        unwrap().subscribe(s);
    }

    /**
     * @deprecated It is not intended for users to subscribe directly to this Flux
     */
    @Deprecated
    default <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
        return unwrap().subscribeWith(subscriber);
    }

    /**
     * Return the underlying Flux backing this AloExtendedFlux
     */
    Flux<? extends T> unwrap();

    /**
     * Transform the items emitted by this {@link AloExtendedFlux} by applying a synchronous
     * function to each item.
     *
     * @param mapper the synchronous transforming {@link Function}
     * @param <V>    the transformed type
     * @return a transformed {@link AloExtendedFlux}
     * @see Flux#map(Function)
     */
    <V> AloExtendedFlux<V> map(Function<? super T, ? extends V> mapper);

    /**
     * Transform the elements emitted by this {@link AloExtendedFlux} asynchronously into
     * Publishers of {@link Alo}, then flatten these inner Publishers in to a single
     * {@link AloFlux}, which allows them to interleave
     *
     * @param mapper {@link Function} to transform each emission into {@link Publisher} of {@link Alo}
     * @param <V>    the type referenced by each resulting {@link Alo}
     * @return a new {@link AloFlux} of the merged results
     * @see Flux#flatMap(Function)
     */
    <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper);
}
