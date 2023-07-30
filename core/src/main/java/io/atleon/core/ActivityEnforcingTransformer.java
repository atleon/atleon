package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

final class ActivityEnforcingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final ActivityEnforcementConfig config;

    ActivityEnforcingTransformer(ActivityEnforcementConfig config) {
        this.config = config;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        // When enabled, transformation is deferred so each subscription will have its own
        // 'lastActive' reference
        return config.isEnabled()
            ? Flux.from(publisher).transformDeferred(this::enforceActivity)
            : publisher;
    }

    private Flux<T> enforceActivity(Publisher<T> publisher) {
        AtomicLong lastActiveEpochMilli = new AtomicLong(System.currentTimeMillis());
        return Flux.merge(createInactivityError(lastActiveEpochMilli::get), publisher)
            .doOnEach(signal -> lastActiveEpochMilli.set(System.currentTimeMillis()));
    }

    private Mono<T> createInactivityError(LongSupplier lastActiveEpochMilli) {
        return Flux.interval(config.getDelay(), config.getInterval())
            .map(i -> lastActiveEpochMilli.getAsLong())
            .filter(this::hasBecomeInactiveSince)
            .next()
            .flatMap(this::createInactivityError);
    }

    private boolean hasBecomeInactiveSince(long lastActiveEpochMilli) {
        Duration durationSinceLastActivity = Duration.ofMillis(System.currentTimeMillis() - lastActiveEpochMilli);
        return durationSinceLastActivity.compareTo(config.getMaxInactivity()) > 0;
    }

    private Mono<T> createInactivityError(long lastActiveEpochMilli) {
        return Mono.error(new InactiveStreamException(config.getName(), config.getMaxInactivity(), lastActiveEpochMilli));
    }

    private static final class InactiveStreamException extends TimeoutException {

        public InactiveStreamException(String name, Duration maxInactivity, long lastActiveEpochMilli) {
            super(String.format("Stream=%s has been inactive for longer than duration=%s with lastActive=%s",
                name, maxInactivity, Instant.ofEpochMilli(lastActiveEpochMilli)));
        }
    }
}
