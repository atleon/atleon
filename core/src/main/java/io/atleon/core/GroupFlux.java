package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * An {@link Alo}-aware Flux that is known to be emitting some form of grouping of items. This
 * grouping is typically some other {@link Publisher}.
 *
 * @param <T> the type of groups emitted by this {@link GroupFlux}
 */
public class GroupFlux<T> implements AloExtendedFlux<T> {

    private final Flux<? extends T> wrapped;

    private final int cardinality;

    GroupFlux(Flux<? extends T> wrapped, int cardinality) {
        this.wrapped = wrapped;
        this.cardinality = cardinality;
    }

    /**
     * Return the underlying Flux backing this GroupFlux
     */
    @Override
    public Flux<? extends T> unwrap() {
        return wrapped;
    }

    /**
     * Transform the items emitted by this {@link GroupFlux} by applying a synchronous
     * function to each item.
     *
     * @param mapper the synchronous transforming {@link Function}
     * @param <V>    the transformed type
     * @return a transformed {@link GroupFlux}
     * @see Flux#map(Function)
     */
    @Override
    public final <V> GroupFlux<V> map(Function<? super T, ? extends V> mapper) {
        return new GroupFlux<>(wrapped.map(mapper), cardinality);
    }

    /**
     * Transform the elements emitted by this {@link GroupFlux} asynchronously into
     * Publishers of {@link Alo}, then flatten these inner Publishers in to a single
     * {@link AloFlux}, which allows them to interleave
     *
     * @param mapper {@link Function} to transform each emission into {@link Publisher} of {@link Alo}
     * @param <V>    the type referenced by each resulting {@link Alo}
     * @return a new {@link AloFlux} of the merged results
     * @see Flux#flatMap(Function)
     */
    @Override
    public final <V> AloFlux<V> flatMapAlo(Function<? super T, ? extends Publisher<Alo<V>>> mapper) {
        return wrapped.flatMap(mapper, cardinality).as(AloFlux::wrap);
    }
}
