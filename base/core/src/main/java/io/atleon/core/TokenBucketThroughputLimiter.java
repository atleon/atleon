package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * Limits throughput using a continuously replenished token bucket per mount. Supports weighted
 * elements and dynamic rate limits, delaying reservations until capacity is available and
 * signaling a blocked state when the rate is zero.
 *
 * @param <T> The type of elements upon which throughput limiting is enforced
 */
final class TokenBucketThroughputLimiter<T> implements ReactiveThroughputLimiter<T> {

    private final Flux<Rate> limits;

    private final ToIntFunction<? super T> weighting;

    private final Scheduler scheduler;

    private TokenBucketThroughputLimiter(
            Publisher<Rate> limits, ToIntFunction<? super T> weighting, Scheduler scheduler) {
        this.limits = Flux.from(limits).defaultIfEmpty(Rate.infinite());
        this.weighting = weighting;
        this.scheduler = scheduler;
    }

    static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    @Override
    public Mount<T> mount(Consumer<? super State> currentStateConsumer, Consumer<? super Throwable> errorConsumer) {
        return new TokenBucketMount(currentStateConsumer, errorConsumer);
    }

    static final class Builder<T> {

        private Publisher<Rate> limits = Flux.empty();

        private ToIntFunction<? super T> weighting = __ -> 1;

        private Scheduler scheduler = Schedulers.parallel();

        public TokenBucketThroughputLimiter<T> build() {
            return new TokenBucketThroughputLimiter<>(limits, weighting, scheduler);
        }

        public Builder<T> limit(Rate limit) {
            return limits(Flux.just(limit));
        }

        public Builder<T> limits(Publisher<Rate> limits) {
            this.limits = limits;
            return this;
        }

        public Builder<T> weighting(ToIntFunction<? super T> weighting) {
            this.weighting = weighting;
            return this;
        }

        public Builder<T> scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
    }

    private final class TokenBucketMount implements Mount<T> {

        private final Consumer<? super State> currentStateConsumer;

        private final SerialQueue<Consumer<TokenBucket>> bucketQueue;

        private final Disposable limitSubscription;

        public TokenBucketMount(
                Consumer<? super State> currentStateConsumer, Consumer<? super Throwable> errorConsumer) {
            this.currentStateConsumer = currentStateConsumer;
            this.bucketQueue = SerialQueue.on(new TokenBucket(scheduler, Rate.infinite()));
            this.limitSubscription = limits.subscribe(this::updateLimit, errorConsumer);
        }

        @Override
        public Mono<?> reserveCapacity(T t) {
            int weight = weighting.applyAsInt(t);
            return reserveTokens(bucketQueue, weight)
                    .delayUntil(it -> Mono.delay(it, scheduler))
                    .doOnCancel(() -> replenishTokens(bucketQueue, weight));
        }

        @Override
        public void unmount() {
            limitSubscription.dispose();
        }

        private void updateLimit(Rate limit) {
            // Implementation notes:
            // 1. Full operation is serialized on (possible) bucket configuration update such that
            //    notification is causal.
            // 2. Bucket configuration is only updated when the limit is positive. It is the
            //    client's responsibility to (eventually) respect "blocked" state.
            bucketQueue.addAndDrain(bucket -> {
                if (!limit.isZero()) {
                    bucket.updateLimit(limit);
                }
                currentStateConsumer.accept((ReactiveThroughputLimiter.State) limit::isZero);
            });
        }

        private static Mono<Duration> reserveTokens(SerialQueue<Consumer<TokenBucket>> queue, int weight) {
            return Mono.create(sink -> queue.addAndDrain(bucket -> {
                try {
                    long delayNanos = bucket.reserveTokens(weight);
                    if (delayNanos > 0) {
                        sink.success(Duration.ofNanos(delayNanos));
                    } else if (weight >= 0) {
                        sink.success();
                    } else {
                        throw new IllegalArgumentException("Weight must be non-negative: " + weight);
                    }
                } catch (Throwable error) {
                    sink.error(error);
                }
            }));
        }

        private static void replenishTokens(SerialQueue<Consumer<TokenBucket>> queue, int weight) {
            queue.addAndDrain(bucket -> bucket.replenishTokens(weight));
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
     * of available capacity when the enforced limit changes between finite rates. Resuming finite
     * limiting after an infinite rate starts with a full bucket.
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

        public long reserveTokens(int weight) {
            // Short circuit if weight is non-positive or number of tokens is infinite
            if (weight <= 0 || count == Long.MAX_VALUE) {
                return 0L;
            }

            long nowNanos = now();

            // "Clamp" depletion in case this is the first request (in a "while")
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

        public void replenishTokens(int weight) {
            // Short circuit if weight is non-positive or number of tokens is infinite
            if (weight <= 0 || count == Long.MAX_VALUE) {
                return;
            }

            // Pull depletion time backward based on weighted replenishment
            long adjustmentNanos = depletionRemainder - Math.multiplyExact(durationNanos, weight);
            depletionNanos += Math.floorDiv(adjustmentNanos, count);
            depletionRemainder = Math.floorMod(adjustmentNanos, count);
        }

        public void updateLimit(Rate limit) {
            long nowNanos = now();
            long remainingNanos = count != Long.MAX_VALUE
                    ? Math.max(depletionNanos, nowNanos - durationNanos) - nowNanos
                    : -durationNanos;
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
