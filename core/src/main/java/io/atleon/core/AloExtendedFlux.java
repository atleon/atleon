package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

import java.util.function.Function;

/**
 * An {@link Alo}-aware extension of {@link Flux}. All <pre>*Alo</pre> methods are delegated to
 * Flux and return {@link AloExtendedFlux}.
 */
public class AloExtendedFlux<T> extends FluxOperator<T, T> {

    AloExtendedFlux(Flux<? extends T> source) {
        super(source);
    }

    /**
     * See {@link Flux#map(Function)}
     */
    public final <V> AloExtendedFlux<V> mapAlo(Function<? super T, ? extends V> mapper) {
        return new AloExtendedFlux<>(source.map(mapper));
    }

    /**
     * See {@link Flux#flatMap(Function)}
     */
    public final <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper) {
        return source.flatMap(mapper).as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#flatMap(Function, int)}
     */
    public final <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper, int concurrency) {
        return source.flatMap(mapper, concurrency).as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#flatMap(Function, int, int)}
     */
    public final <V> AloFlux<V>
    flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper, int concurrency, int prefetch) {
        return source.flatMap(mapper, concurrency, prefetch).as(AloFlux::wrap);
    }

    /**
     * See {@link Flux#transform(Function)}
     */
    public final <V> AloFlux<V>
    transformAlo(Function<? super AloExtendedFlux<T>, ? extends Publisher<Alo<V>>> transformer) {
        return AloFlux.wrap(transformer.apply(this));
    }

    /**
     * See {@link Flux#subscribe(CoreSubscriber)}
     */
    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        source.subscribe(actual);
    }
}
