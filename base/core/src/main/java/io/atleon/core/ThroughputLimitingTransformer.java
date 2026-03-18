package io.atleon.core;

import io.github.bucket4j.BandwidthBuilder;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.TokensInheritanceStrategy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Transformer used to limit the throughput of some given source publisher.
 *
 * @param <T> The type of elements upon which throughput limiting is enforced
 * @param <V> A container of the elements being limited, which may be the element itself
 */
public final class ThroughputLimitingTransformer<T, V> implements Function<Publisher<? extends V>, Publisher<V>> {

    private final Applicator<T, V> applicator;

    private final Publisher<Rate> limits;

    private final ToIntFunction<? super T> weighting;

    private final Scheduler scheduler;

    private ThroughputLimitingTransformer(
            Applicator<T, V> applicator,
            Publisher<Rate> limits,
            ToIntFunction<? super T> weighting,
            Scheduler scheduler) {
        this.applicator = applicator;
        this.limits = limits;
        this.weighting = weighting;
        this.scheduler = scheduler;
    }

    static <T> ThroughputLimitingTransformer.Builder<T, T> concatMap(int prefetch) {
        return new Builder<>((flux, mapper) -> flux.concatMap(mapper, prefetch));
    }

    static <T> ThroughputLimitingTransformer.Builder<T, Flux<T>> innerConcatMap(int prefetch) {
        return innerConcatMap(Function.identity(), prefetch, (__, delayedInner) -> delayedInner);
    }

    /**
     * Creates a {@link ThroughputLimitingTransformer} that can be applied to a Publisher of
     * Publishers, across which is applied a cumulative dynamic throughput limit. Note that this
     * transformation will result in making the transformed publisher an infinite publisher, i.e.
     * it will never signal onComplete. The reason for this is that the throughput limiting
     * resources have their lifecycle tied to the lifecycle of the outer stream, so in order to
     * keep the throughput limiting dynamic, we must ensure the outer stream never completes.
     */
    static <T, V extends Publisher<T>> ThroughputLimitingTransformer.Builder<T, V> innerConcatMap(
            Function<V, ? extends Publisher<T>> innerUnwrapper, int prefetch, BiFunction<V, Flux<T>, V> rewrapper) {
        // Implementation notes:
        // 1. Cancellation of outer stream requires explicit propagation to inner streams. This is
        //    accomplished with an empty sink that inner publishers subscribe to for cancellation.
        //    Without this, the inner publishers would not terminate if (for example) the outer
        //    stream is canceled due to having the throughput limit set to zero.
        // 2. Applicator ensures subscribed stream is infinite by concatenating with Flux.never().
        //    This makes it safe to attach resources (like dynamic limit updates) to the lifecycle
        //    of the outer stream. Note that this cardinality is otherwise already a technical
        //    possibility, since setting limit to zero causes subscription to a "never" publisher.
        return new Builder<>((outer, mapper) -> {
            Sinks.Empty<Void> cancellation = Sinks.empty();
            return outer.concatWith(Flux.never())
                    .doOnCancel(cancellation::tryEmitEmpty)
                    .map(inner -> {
                        Flux<T> delayedInner = Flux.from(innerUnwrapper.apply(inner))
                                .concatMap(mapper, prefetch)
                                .takeUntilOther(cancellation.asMono());
                        return rewrapper.apply(inner, delayedInner);
                    });
        });
    }

    @Override
    public Flux<V> apply(Publisher<? extends V> publisher) {
        // Implementation notes:
        // 1. We create a "shared" publisher of limits such that it is only subscribed to once per
        //    subscription to the source publisher. We cache the latest value such that a dynamic
        //    constraint is guaranteed to see the latest limit upon subscription.
        // 2. Limits are only emitted when they change between zero and non-zero states, which
        //    controls cancellation and subscription to the source publisher. Limit changes within
        //    positive values are reflected by active changes to the enforced limit within the
        //    dynamic throughput constraint.
        // 3. We ensure that at least one constraint is always emitted, even if the limit publisher
        //    is empty, in which case the source publisher is subscribed to with a permanently
        //    infinite limit.
        Flux<Rate> sharedLimits = Flux.from(limits).replay(1).refCount();
        return sharedLimits
                .distinctUntilChanged(Rate::isZero)
                .map(it -> it.isZero() ? new ZeroThroughputConstraint<V>() : new DynamicThroughputConstraint(it))
                .defaultIfEmpty(new UnlimitedThroughputConstraint<>())
                .switchMap(constraint -> constraint.apply(publisher, sharedLimits));
    }

    public static final class Builder<T, V> {

        private final Applicator<T, V> applicator;

        private Publisher<Rate> limits = Mono.empty();

        private ToIntFunction<? super T> weighting = __ -> 1;

        private Scheduler scheduler = Schedulers.parallel();

        private Builder(Applicator<T, V> applicator) {
            this.applicator = applicator;
        }

        public ThroughputLimitingTransformer<T, V> build() {
            return new ThroughputLimitingTransformer<>(applicator, limits, weighting, scheduler);
        }

        public Builder<T, V> limits(Publisher<Rate> limits) {
            this.limits = limits;
            return this;
        }

        public Builder<T, V> weighting(ToIntFunction<? super T> weighting) {
            this.weighting = weighting;
            return this;
        }

        public Builder<T, V> scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
    }

    @FunctionalInterface
    private interface Applicator<T, V> {

        Flux<V> apply(Flux<? extends V> source, Function<T, ? extends Publisher<T>> delayer);
    }

    /**
     * Encapsulates if and how to apply a throughput limitation to a source publisher, given a
     * stream of throughput limits.
     */
    private interface ThroughputConstraint<T> {

        Publisher<? extends T> apply(Publisher<? extends T> publisher, Flux<Rate> limits);
    }

    private static final class ZeroThroughputConstraint<T> implements ThroughputConstraint<T> {

        @Override
        public Publisher<? extends T> apply(Publisher<? extends T> publisher, Flux<Rate> limits) {
            return Flux.never();
        }
    }

    private final class DynamicThroughputConstraint implements ThroughputConstraint<V> {

        private final Rate initialLimit;

        private DynamicThroughputConstraint(Rate initialLimit) {
            this.initialLimit = initialLimit;
        }

        @Override
        public Publisher<? extends V> apply(Publisher<? extends V> publisher, Flux<Rate> limits) {
            // Implementation notes:
            // 1. Limits are kept dynamic/updated on the created grantor by subscribing to the
            //    limits with a side effect on the grantor.
            // 2. The limit updating process is characterized as publisher that never completes,
            //    which allows us to use it as a "takeUntilOther" condition on the source
            //    publisher. This makes it such that errors on the limit stream are propagated
            //    to the main publisher, and termination of the main publisher also terminates
            //    the limit updating process. Using "takeUntilOther" rather than "merge" prevents
            //    inadvertent introduction of an unnecessary prefetch buffer.
            ThroughputGrantor grantor = new Bucket4jThroughputGrantor(initialLimit);
            Mono<Void> limitUpdating = limits.skipWhile(initialLimit::equals)
                    .distinctUntilChanged()
                    .doOnNext(grantor::updateLimit)
                    .thenEmpty(Mono.never()); // Don't cancel main if limit updating completes
            return applicator
                    .apply(Flux.from(publisher), it -> maybeDelay(grantor, it))
                    .takeUntilOther(limitUpdating);
        }

        private Mono<T> maybeDelay(ThroughputGrantor grantor, T t) {
            int weight = weighting.applyAsInt(t);
            long delayNanos = grantor.reserveCapacity(weight);
            return delayNanos <= 0 ? Mono.just(t) : Mono.just(t).delayElement(Duration.ofNanos(delayNanos), scheduler);
        }
    }

    private static final class UnlimitedThroughputConstraint<T> implements ThroughputConstraint<T> {

        @Override
        public Publisher<? extends T> apply(Publisher<? extends T> publisher, Flux<Rate> limits) {
            return publisher;
        }
    }

    /**
     * API through which throughput capacity is reserved and updated. Reservation of throughput
     * capacity may require waiting/delaying until the return number of nanoseconds has elapsed.
     * Limitation of available throughput capacity can be dynamic, in which case the grantor will
     * update its internal state to reflect the new limit.
     */
    private interface ThroughputGrantor {

        /**
         * Request reservation of throughput capacity for the provided throughput weight. Returns
         * the number of nanoseconds to wait before safely using the reserved capacity.
         *
         * @param weight The weight of throughput capacity to reserve
         * @return The number of nanoseconds to wait before safely using the reserved capacity
         */
        long reserveCapacity(int weight);

        /**
         * Requests a change in any configured throughput limit.
         *
         * @param limit The updated throughput limit to enforce from this grantor
         */
        void updateLimit(Rate limit);
    }

    private static final class Bucket4jThroughputGrantor implements ThroughputGrantor {

        // Nullity signifies the difference between infinite and non-infinite limits
        private volatile Bucket bucket;

        public Bucket4jThroughputGrantor(Rate initialLimit) {
            this.bucket = initialLimit.isInfinite() ? null : newBucket(initialLimit);
        }

        @Override
        public long reserveCapacity(int weight) {
            Bucket nvBucket = bucket;
            return nvBucket != null ? nvBucket.consumeIgnoringRateLimits(weight) : 0L;
        }

        @Override
        public void updateLimit(Rate limit) {
            if (limit.isInfinite()) {
                bucket = null;
            } else if (bucket != null) {
                updateLimit(bucket, limit);
            } else {
                bucket = newBucket(limit);
            }
        }

        private static Bucket newBucket(Rate limit) {
            return Bucket.builder()
                    .addLimit(it -> applyLimit(it, limit))
                    .withNanosecondPrecision()
                    .build();
        }

        private static void updateLimit(Bucket bucket, Rate limit) {
            BucketConfiguration configuration = BucketConfiguration.builder()
                    .addLimit(it -> applyLimit(it, limit))
                    .build();
            bucket.replaceConfiguration(configuration, TokensInheritanceStrategy.PROPORTIONALLY);
        }

        private static BandwidthBuilder.BandwidthBuilderBuildStage applyLimit(
                BandwidthBuilder.BandwidthBuilderCapacityStage bandwidthBuilder, Rate limit) {
            return bandwidthBuilder.capacity(limit.count()).refillGreedy(limit.count(), limit.duration());
        }
    }
}
