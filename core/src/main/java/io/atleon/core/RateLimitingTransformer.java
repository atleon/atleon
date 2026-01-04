package io.atleon.core;

import com.google.common.util.concurrent.RateLimiter;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

final class RateLimitingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final UnaryOperator<Publisher<T>> rateLimiter;

    private final Scheduler scheduler;

    RateLimitingTransformer(RateLimitingConfig config, Scheduler scheduler) {
        this.rateLimiter = config.isEnabled() ? createRateLimiter(config) : UnaryOperator.identity();
        this.scheduler = scheduler;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return rateLimiter.apply(publisher);
    }

    private UnaryOperator<Publisher<T>> createRateLimiter(RateLimitingConfig config) {
        RateLimiter rateLimiter = RateLimiter.create(config.getPermitsPerSecond());
        return publisher ->
                Flux.from(publisher).publishOn(scheduler, config.getPrefetch()).doOnNext(t -> rateLimiter.acquire());
    }
}
