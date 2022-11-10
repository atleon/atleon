package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An {@link Alo}-aware wrapper of a {@link Flux}. All {@code *Extended} methods are delegated to
 * Flux and return {@link AloExtendedFlux}. All {@code *Alo} methods also delegate to Flux, but
 * map to result types that allow them to be wrapped as {@link AloFlux} and are wrapped as such.
 */
public class AloExtendedFlux<T> implements Publisher<T> {

    private final Flux<? extends T> wrapped;

    AloExtendedFlux(Flux<? extends T> wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Wraps a Publisher as an Alo-aware Flux
     */
    public static <T> AloExtendedFlux<T> wrap(Publisher<? extends T> publisher) {
        return publisher instanceof AloExtendedFlux
            ? AloExtendedFlux.class.cast(publisher)
            : new AloExtendedFlux<>(Flux.from(publisher));
    }

    /**
     * Return the underlying Flux backing this AloExtendedFlux
     */
    public Flux<? extends T> unwrap() {
        return wrapped;
    }

    /**
     * See {@link Flux#map(Function)}
     */
    public final <V> AloExtendedFlux<V> mapExtended(Function<? super T, ? extends V> mapper) {
        return new AloExtendedFlux<>(wrapped.map(mapper));
    }

    /**
     * See {@link Flux#flatMap(Function)}
     */
    public final <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper) {
        return wrapped.flatMap(mapper).as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#flatMap(Function, int)}
     */
    public final <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper, int concurrency) {
        return wrapped.flatMap(mapper, concurrency).as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#flatMap(Function, int, int)}
     */
    public final <V> AloFlux<V>
    flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper, int concurrency, int prefetch) {
        return wrapped.flatMap(mapper, concurrency, prefetch).as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#transform(Function)}
     */
    public final <V> AloFlux<V>
    transformAlo(Function<? super AloExtendedFlux<T>, ? extends Publisher<Alo<V>>> transformer) {
        return AloFlux.wrap(transformer.apply(this));
    }

    /**
     * See {@link Flux#subscribe(Consumer)}
     */
    public Disposable subscribe(Consumer<? super T> consumer) {
        return wrapped.subscribe(consumer);
    }

    /**
     * See {@link Flux#subscribe(Consumer, Consumer)}
     */
    public Disposable subscribe(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
        return wrapped.subscribe(consumer, errorConsumer);
    }

    /**
     * See {@link Flux#subscribe(Subscriber)}
     */
    @Override
    public void subscribe(Subscriber<? super T> s) {
        wrapped.subscribe(s);
    }

    public <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
        return wrapped.subscribeWith(subscriber);
    }
}
