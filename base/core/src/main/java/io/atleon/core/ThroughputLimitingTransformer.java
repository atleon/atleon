package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Transformer used to limit the throughput of some given source publisher.
 *
 * @param <T> The type of elements upon which throughput limiting is enforced
 * @param <V> A container of the elements being limited, which may be the element itself
 */
public final class ThroughputLimitingTransformer<T, V> implements Function<Publisher<? extends V>, Publisher<V>> {

    private final DelayApplicator<T, V> delayApplicator;

    private final Publisher<Rate> limits;

    private final ToIntFunction<? super T> weighting;

    private final Scheduler scheduler;

    private final int prefetch;

    private ThroughputLimitingTransformer(
            DelayApplicator<T, V> delayApplicator,
            Publisher<Rate> limits,
            ToIntFunction<? super T> weighting,
            Scheduler scheduler,
            int prefetch) {
        this.delayApplicator = delayApplicator;
        this.limits = limits;
        this.weighting = weighting;
        this.scheduler = scheduler;
        this.prefetch = prefetch;
    }

    static <T> ThroughputLimitingTransformer.Builder<T, T> concatMap() {
        return new Builder<>(DelayApplicator.concatMap());
    }

    static <T> ThroughputLimitingTransformer.Builder<T, Flux<T>> innerConcatMap() {
        return innerConcatMap(Function.identity(), (__, delayedInner) -> delayedInner);
    }

    /**
     * Creates a {@link ThroughputLimitingTransformer} that can be applied to a Publisher of
     * Publishers, across which is applied a cumulative dynamic throughput limit. Note that when a
     * non-empty limit publisher is used, this transformation will result in making the transformed
     * publisher an infinite publisher, i.e. it will never signal onComplete. The reason for this
     * is that the throughput limiting resources have their lifecycle tied to the lifecycle of the
     * outer stream, so in order to keep the throughput limiting dynamic, we must ensure the outer
     * stream never completes.
     */
    static <T, V extends Publisher<T>> ThroughputLimitingTransformer.Builder<T, V> innerConcatMap(
            Function<V, ? extends Publisher<T>> innerUnwrapper, BiFunction<V, ? super Flux<T>, V> innerRewrapper) {
        // Implementation notes:
        // 1. Cancellation of outer stream requires explicit propagation to inner streams. This is
        //    accomplished with an empty sink that inner publishers subscribe to for cancellation.
        //    Without this, the inner publishers would not terminate if (for example) the outer
        //    stream is canceled due to having the throughput limit set to zero.
        // 2. When this transformer is applied with a non-empty limit publisher, this Applicator
        //    ensures the subscribed stream is infinite by concatenating with Flux.never(). This
        //    makes it safe to attach resources (like dynamic limit updates) to the lifecycle of
        //    the outer stream. Note that this cardinality is otherwise already a technical
        //    possibility, since setting limit to zero causes subscription to a "never" publisher.
        return new Builder<>((outer, delayer, prefetch) -> {
            Sinks.Empty<Void> cancellation = Sinks.empty();
            Flux<V> decoratedOuter = outer.concatWith(Flux.never()).doOnCancel(cancellation::tryEmitEmpty);
            BiFunction<V, Flux<T>, V> decoratedInnerRewrapper = (originalInner, delayedElements) ->
                    innerRewrapper.apply(originalInner, delayedElements.takeUntilOther(cancellation.asMono()));
            return DelayApplicator.innerConcatMap(innerUnwrapper, decoratedInnerRewrapper)
                    .apply(decoratedOuter, delayer, prefetch);
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

        private final DelayApplicator<T, V> delayApplicator;

        private Publisher<Rate> limits = Mono.empty();

        private ToIntFunction<? super T> weighting = __ -> 1;

        private Scheduler scheduler = Schedulers.parallel();

        private int prefetch = 0;

        private Builder(DelayApplicator<T, V> delayApplicator) {
            this.delayApplicator = delayApplicator;
        }

        public ThroughputLimitingTransformer<T, V> build() {
            return new ThroughputLimitingTransformer<>(delayApplicator, limits, weighting, scheduler, prefetch);
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

        public Builder<T, V> prefetch(int prefetch) {
            this.prefetch = prefetch;
            return this;
        }
    }

    /**
     * Interface through which per-element delays can be applied to a source, which is assumed to
     * emit either elements of the delayed type, or elements containing/emitting that type (i.e. a
     * Grouped Flux of elements).
     */
    @FunctionalInterface
    private interface DelayApplicator<T, V> {

        static <T> DelayApplicator<T, T> concatMap() {
            return Flux::concatMap;
        }

        static <T, V> DelayApplicator<T, V> innerConcatMap(
                Function<V, ? extends Publisher<T>> innerUnwrapper, BiFunction<V, ? super Flux<T>, V> innerRewrapper) {
            return (outer, delayer, prefetch) -> outer.map(originalInner -> {
                Flux<T> elements = Flux.from(innerUnwrapper.apply(originalInner));
                return innerRewrapper.apply(originalInner, elements.concatMap(delayer, prefetch));
            });
        }

        Flux<V> apply(Flux<V> source, Function<T, ? extends Publisher<T>> delayer, int prefetch);
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
            // 2. Zero and consecutive equal limits (including starting limits equal to the initial
            //    limit) are skipped to avoid needless resetting/configuration on the grantor.
            // 3. The limit updating process is characterized as a publisher that never completes,
            //    which allows us to use it as a "takeUntilOther" condition on the source
            //    publisher. This makes it such that errors on the limit stream are propagated
            //    to the main publisher, and termination of the main publisher also terminates
            //    the limit updating process. Using "takeUntilOther" rather than "merge" prevents
            //    inadvertent introduction of an unnecessary prefetch buffer.
            TokenBucketThroughputGrantor<T> grantor = new TokenBucketThroughputGrantor<>(weighting, scheduler);
            Mono<Void> limitUpdating = limits.filter(it -> !it.isZero())
                    .startWith(initialLimit)
                    .distinctUntilChanged()
                    .doOnNext(grantor::updateLimit)
                    .thenEmpty(Mono.never());
            return delayApplicator
                    .apply(Flux.from(publisher), it -> maybeDelay(grantor, it), prefetch)
                    .takeUntilOther(limitUpdating);
        }

        private Mono<T> maybeDelay(ThroughputGrantor<T> grantor, T t) {
            return grantor.reserveCapacity(t)
                    .delayUntil(it -> it > 0 ? Mono.delay(Duration.ofNanos(it), scheduler) : Mono.empty())
                    .thenReturn(t);
        }
    }

    private static final class UnlimitedThroughputConstraint<T> implements ThroughputConstraint<T> {

        @Override
        public Publisher<? extends T> apply(Publisher<? extends T> publisher, Flux<Rate> limits) {
            return publisher;
        }
    }

    /**
     * API through which throughput capacity is reserved for elements. Reservation of throughput
     * capacity may require waiting until the emitted number of nanoseconds has elapsed.
     */
    private interface ThroughputGrantor<T> {

        /**
         * Creates a reservation of throughput capacity for the provided element. Returns
         * a {@link Mono} that may emit the number of nanoseconds to wait before safely using the
         * reserved capacity. Emitting nothing or a non-positive value is treated as "no wait
         * necessary".
         *
         * @param t The element for which to reserve throughput capacity
         * @return A {@link Mono} emitting number of nanoseconds to wait before using reservation
         */
        Mono<Long> reserveCapacity(T t);
    }

    private static final class TokenBucketThroughputGrantor<T> implements ThroughputGrantor<T> {

        private final ToIntFunction<? super T> weighting;

        private final Scheduler scheduler;

        // Null means the limit is infinite, so reservations skip weight calculation and delay
        private volatile SerialQueue<Consumer<TokenBucket>> bucketQueue = null;

        public TokenBucketThroughputGrantor(ToIntFunction<? super T> weighting, Scheduler scheduler) {
            this.weighting = weighting;
            this.scheduler = scheduler;
        }

        @Override
        public Mono<Long> reserveCapacity(T t) {
            SerialQueue<Consumer<TokenBucket>> nvBucketQueue = bucketQueue;
            return nvBucketQueue != null ? reserveCapacity(nvBucketQueue, weighting.applyAsInt(t)) : Mono.empty();
        }

        public void updateLimit(Rate limit) {
            SerialQueue<Consumer<TokenBucket>> nvBucketQueue = bucketQueue;
            if (limit.isInfinite()) {
                bucketQueue = null;
            } else if (nvBucketQueue != null) {
                nvBucketQueue.addAndDrain(it -> it.updateLimit(limit));
            } else {
                bucketQueue = SerialQueue.on(new TokenBucket(scheduler, limit));
            }
        }

        private static Mono<Long> reserveCapacity(SerialQueue<Consumer<TokenBucket>> queue, int weight) {
            return Mono.create(sink -> queue.addAndDrain(bucket -> {
                try {
                    if (weight > 0) {
                        sink.success(bucket.reserveCapacity(weight));
                    } else if (weight == 0) {
                        sink.success();
                    } else {
                        throw new IllegalArgumentException("Weight must be non-negative: " + weight);
                    }
                } catch (Throwable error) {
                    sink.error(error);
                }
            }));
        }
    }

    /**
     * A token bucket whose capacity is continuously (rather than discretely) replenished, i.e. a
     * bucket holding up to {@link Rate#count()} tokens which are replenished smoothly over
     * {@link Rate#duration()}. Reservation of capacity is always granted, and results in the
     * number of nanoseconds that must elapse before the reserved capacity is safely usable.
     *
     * <p>Rather than tracking available tokens directly, this bucket tracks the point in time at
     * which it is depleted, such that the number of tokens available at any given time is
     * {@code min(count, ((now - depletion) * count) / duration)}. Reserving capacity is therefore
     * implemented by pushing the point of depletion further in to the future, and the resulting
     * delay is however much of that push extends beyond the current time. Note that this also
     * means a full bucket is equivalent to a point of depletion exactly one duration in the past,
     * which is used both to bound available capacity when reserving and to preserve the proportion
     * of available capacity when the enforced limit changes.
     *
     * <p>This bucket is not thread safe, and therefore requires that invocation of its methods is
     * serialized by the caller.
     */
    private static final class TokenBucket {

        private final Scheduler scheduler;

        private long count;

        private long durationNanos;

        private long depletionNanos;

        // Remainder of division by count, which keeps replenishment exact for counts that do not
        // evenly divide their duration
        private long depletionRemainder;

        public TokenBucket(Scheduler scheduler, Rate limit) {
            this.scheduler = scheduler;
            this.count = limit.count();
            this.durationNanos = limit.duration().toNanos();
            this.depletionNanos = now() - durationNanos;
            this.depletionRemainder = 0L;
        }

        public long reserveCapacity(int weight) {
            long nowNanos = now();

            // Check if this is the first request (in a "while") and if so, reset depletion time
            long fullDepletionNanos = nowNanos - durationNanos;
            if (depletionNanos < fullDepletionNanos) {
                depletionNanos = fullDepletionNanos;
                depletionRemainder = 0L;
            }

            // Push the depletion time forward based on weighted reservation
            long pushNanos = Math.multiplyExact(durationNanos, weight) + depletionRemainder;
            depletionNanos += pushNanos / count;
            depletionRemainder = pushNanos % count;

            // Delay is however far into the future the depletion time is (negative if no delay)
            return (depletionNanos - nowNanos) + (depletionRemainder > 0L ? 1L : 0L);
        }

        public void updateLimit(Rate limit) {
            long nowNanos = now();
            long remainingNanos = Math.max(depletionNanos, nowNanos - durationNanos) - nowNanos;
            long limitNanos = limit.duration().toNanos();

            count = limit.count();
            depletionNanos = nowNanos + scale(remainingNanos, limitNanos, durationNanos);
            depletionRemainder = 0L;
            durationNanos = limitNanos;
        }

        private long now() {
            return scheduler.now(TimeUnit.NANOSECONDS);
        }

        /**
         * Returns {@code (nanos * numerator) / denominator}, applied separately to the quotient
         * and remainder of {@code nanos / denominator} such that the intermediate multiplication
         * cannot overflow. Note that a naive multiplication would overflow once the point of
         * depletion is more than roughly nine seconds in the future, which is reachable at low
         * limits with many concurrently reserving streams.
         */
        private static long scale(long nanos, long numerator, long denominator) {
            return ((nanos / denominator) * numerator) + (((nanos % denominator) * numerator) / denominator);
        }
    }
}
