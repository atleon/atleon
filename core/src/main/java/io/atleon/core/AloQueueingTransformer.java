package io.atleon.core;

import org.reactivestreams.Publisher;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Transformer used on Publishers of items to create {@link Alo} items where acknowledgement is
 * "queued" such that acknowledgement order may be preserved independently of data processing
 * completion.
 *
 * @param <T> The type of items subscribed to
 * @param <V> The type of items extracted and contained in emitted Alo elements
 */
public final class AloQueueingTransformer<T, V> implements Function<Publisher<T>, Publisher<Alo<V>>> {

    private final Function<T, ?> groupExtractor;

    private final Supplier<? extends AcknowledgementQueue> queueSupplier;

    private final AloComponentExtractor<T, V> componentExtractor;

    private final AloFactory<V> factory;

    private final long maxInFlight;

    private AloQueueingTransformer(
        Function<T, ?> groupExtractor,
        Supplier<? extends AcknowledgementQueue> queueSupplier,
        AloComponentExtractor<T, V> componentExtractor,
        AloFactory<V> factory,
        long maxInFlight
    ) {
        this.groupExtractor = groupExtractor;
        this.queueSupplier = queueSupplier;
        this.componentExtractor = componentExtractor;
        this.factory = factory;
        this.maxInFlight = maxInFlight;
    }

    /**
     * Builds a new {@link AloQueueingTransformer} from the provided {@link AloComponentExtractor}.
     * The returned transformer will result in a single grouping of items, where acknowledgement is
     * executed in the same order as emission, and the number of in-flight messages is unbounded.
     *
     * @param componentExtractor Implements how to extract native {@link Alo} components
     * @return A new {@link AloQueueingTransformer}
     * @param <T> The type of items subscribed to
     * @param <V> The type of items extracted and contained in emitted Alo elements
     */
    public static <T, V> AloQueueingTransformer<T, V> create(AloComponentExtractor<T, V> componentExtractor) {
        return new AloQueueingTransformer<>(
            __ -> "singleton",
            OrderManagingAcknowledgementQueue::create,
            componentExtractor,
            ComposedAlo.factory(),
            Long.MAX_VALUE
        );
    }

    public AloQueueingTransformer<T, V> withGroupExtractor(Function<T, ?> groupExtractor) {
        return new AloQueueingTransformer<>(groupExtractor, queueSupplier, componentExtractor, factory, maxInFlight);
    }

    public AloQueueingTransformer<T, V> withFactory(AloFactory<V> factory) {
        return new AloQueueingTransformer<>(groupExtractor, queueSupplier, componentExtractor, factory, maxInFlight);
    }

    public AloQueueingTransformer<T, V> withMaxInFlight(long maxInFlight) {
        return new AloQueueingTransformer<>(groupExtractor, queueSupplier, componentExtractor, factory, maxInFlight);
    }

    @Override
    public Publisher<Alo<V>> apply(Publisher<T> publisher) {
        return new AloQueueingOperator<>(
            publisher,
            groupExtractor,
            queueSupplier,
            componentExtractor,
            factory,
            maxInFlight
        );
    }
}
