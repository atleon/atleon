package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.Function;

/**
 * Transformer that limits throughput by reserving capacity through a {@link ThroughputGrantor}.
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

    private final ThroughputGrantor<T> grantor;

    private final Scheduler scheduler;

    private GrantedThroughputLimitingTransformer(
            DelayApplicator<T, V> delayApplicator, ThroughputGrantor<T> grantor, Scheduler scheduler) {
        this.delayApplicator = delayApplicator;
        this.grantor = grantor;
        this.scheduler = scheduler;
    }

    static <T, V> GrantedThroughputLimitingTransformer.Builder<T, V> newBuilder(DelayApplicator<T, V> delayApplicator) {
        return new Builder<>(delayApplicator);
    }

    @Override
    public Flux<V> apply(Publisher<? extends V> publisher) {
        return delayApplicator.apply(Flux.from(publisher), it -> maybeDelay(grantor, it));
    }

    private Mono<T> maybeDelay(ThroughputGrantor<T> grantor, T t) {
        return grantor.reserveCapacity(t)
                .delayUntil(it -> it > 0 ? Mono.delay(Duration.ofNanos(it), scheduler) : Mono.empty())
                .thenReturn(t);
    }

    static final class Builder<T, V> {

        private final DelayApplicator<T, V> delayApplicator;

        private ThroughputGrantor<T> grantor = __ -> Mono.empty();

        private Scheduler scheduler = Schedulers.parallel();

        private Builder(DelayApplicator<T, V> delayApplicator) {
            this.delayApplicator = delayApplicator;
        }

        public GrantedThroughputLimitingTransformer<T, V> build() {
            return new GrantedThroughputLimitingTransformer<>(delayApplicator, grantor, scheduler);
        }

        public Builder<T, V> grantor(ThroughputGrantor<T> grantor) {
            this.grantor = grantor;
            return this;
        }

        public Builder<T, V> scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
    }
}
