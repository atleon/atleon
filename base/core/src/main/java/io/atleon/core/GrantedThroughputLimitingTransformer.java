package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Transformer that limits throughput by reserving capacity through a {@link ThroughputGrantor}.
 * Each element is forwarded only after its reservation completes successfully; any value emitted
 * by the reservation is ignored. The grantor is responsible for applying any required delay.
 *
 * <p>The configured grantor is shared across all subscriptions and inner streams transformed by
 * this instance and must support concurrent reservations. A separate grantor is not created for
 * each subscription or inner stream.
 *
 * @param <T> The type of elements for which throughput capacity is reserved
 * @param <V> A container of the elements being limited, which may be the element itself
 */
final class GrantedThroughputLimitingTransformer<T, V> implements Function<Publisher<? extends V>, Publisher<V>> {

    private final DelayApplicator<T, V> delayApplicator;

    private final ThroughputGrantor<? super T> grantor;

    private GrantedThroughputLimitingTransformer(
            DelayApplicator<T, V> delayApplicator, ThroughputGrantor<? super T> grantor) {
        this.delayApplicator = delayApplicator;
        this.grantor = grantor;
    }

    static <T, V> GrantedThroughputLimitingTransformer.Builder<T, V> newBuilder(DelayApplicator<T, V> delayApplicator) {
        return new Builder<>(delayApplicator);
    }

    @Override
    public Flux<V> apply(Publisher<? extends V> publisher) {
        return delayApplicator.apply(Flux.from(publisher), this::delayThroughCapacityReservation);
    }

    private Mono<T> delayThroughCapacityReservation(T t) {
        return grantor.reserveCapacity(t).thenReturn(t);
    }

    static final class Builder<T, V> {

        private final DelayApplicator<T, V> delayApplicator;

        private ThroughputGrantor<? super T> grantor = __ -> Mono.empty();

        private Builder(DelayApplicator<T, V> delayApplicator) {
            this.delayApplicator = delayApplicator;
        }

        public GrantedThroughputLimitingTransformer<T, V> build() {
            return new GrantedThroughputLimitingTransformer<>(delayApplicator, grantor);
        }

        public Builder<T, V> grantor(ThroughputGrantor<? super T> grantor) {
            this.grantor = grantor;
            return this;
        }
    }
}
