package io.atleon.core;

import com.google.common.util.concurrent.RateLimiter;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;
import java.util.function.UnaryOperator;

final class RateLimitingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final UnaryOperator<Publisher<T>> rateLimiter;

    RateLimitingTransformer(RateLimitingConfig config) {
        this.rateLimiter = config.isEnabled() ? createRateLimiter(config) : UnaryOperator.identity();
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return rateLimiter.apply(publisher);
    }

    private UnaryOperator<Publisher<T>> createRateLimiter(RateLimitingConfig config) {
        RateLimiter rateLimiter = RateLimiter.create(config.getPermitsPerSecond());
        return publisher -> Flux.from(publisher).doOnNext(t -> rateLimiter.acquire());
    }
}
