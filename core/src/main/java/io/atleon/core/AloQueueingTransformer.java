package io.atleon.core;

import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

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

    private final AcknowledgementQueueMode queueMode;

    private final AloQueueListener listener;

    private final AloComponentExtractor<T, V> componentExtractor;

    private final AloFactory<V> factory;

    private final long maxInFlight;

    private AloQueueingTransformer(
            Function<T, ?> groupExtractor,
            AcknowledgementQueueMode queueMode,
            AloQueueListener listener,
            AloComponentExtractor<T, V> componentExtractor,
            AloFactory<V> factory,
            long maxInFlight) {
        this.groupExtractor = groupExtractor;
        this.queueMode = queueMode;
        this.listener = listener;
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
                AcknowledgementQueueMode.STRICT,
                AloQueueListener.noOp(),
                componentExtractor,
                ComposedAlo.factory(),
                Long.MAX_VALUE);
    }

    public AloQueueingTransformer<T, V> withGroupExtractor(Function<T, ?> groupExtractor) {
        return new AloQueueingTransformer<>(
                groupExtractor, queueMode, listener, componentExtractor, factory, maxInFlight);
    }

    public AloQueueingTransformer<T, V> withQueueMode(AcknowledgementQueueMode queueMode) {
        return new AloQueueingTransformer<>(
                groupExtractor, queueMode, listener, componentExtractor, factory, maxInFlight);
    }

    public AloQueueingTransformer<T, V> withListener(AloQueueListener listener) {
        return new AloQueueingTransformer<>(
                groupExtractor, queueMode, listener, componentExtractor, factory, maxInFlight);
    }

    public AloQueueingTransformer<T, V> withFactory(AloFactory<V> factory) {
        return new AloQueueingTransformer<>(
                groupExtractor, queueMode, listener, componentExtractor, factory, maxInFlight);
    }

    public AloQueueingTransformer<T, V> withMaxInFlight(long maxInFlight) {
        return new AloQueueingTransformer<>(
                groupExtractor, queueMode, listener, componentExtractor, factory, maxInFlight);
    }

    @Override
    public Publisher<Alo<V>> apply(Publisher<T> publisher) {
        return new AloQueueingOperator<>(
                publisher,
                groupExtractor,
                newQueueSupplier(queueMode),
                listener,
                componentExtractor,
                factory,
                maxInFlight);
    }

    private static Supplier<? extends AcknowledgementQueue> newQueueSupplier(AcknowledgementQueueMode mode) {
        return () -> AcknowledgementQueue.create(mode);
    }
}
