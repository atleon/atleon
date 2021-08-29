package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

final class ActivityEnforcingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final ActivityEnforcementConfig config;

    ActivityEnforcingTransformer(ActivityEnforcementConfig config) {
        this.config = config;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled()
            ? Flux.from(publisher).transformDeferred(this::enforceActivity)
            : publisher;
    }

    private Flux<T> enforceActivity(Publisher<T> publisher) {
        AtomicReference<Instant> lastActive = new AtomicReference<>(Instant.now());
        return Flux.merge(createInactivityError(lastActive::get), publisher)
            .doOnEach(signal -> lastActive.set(Instant.now()));
    }

    private Mono<T> createInactivityError(Supplier<Instant> lastActive) {
        return Flux.interval(config.getDelay(), config.getInterval())
            .map(i -> lastActive.get())
            .filter(this::hasBecomeInactiveSince)
            .next()
            .flatMap(this::createInactivityError);
    }

    private boolean hasBecomeInactiveSince(Instant lastActive) {
        return config.getMaxInactivity().compareTo(Duration.between(lastActive, Instant.now())) < 0;
    }

    private Mono<T> createInactivityError(Instant lastActive) {
        return Mono.error(new InactiveStreamException(config.getName(), config.getMaxInactivity(), lastActive));
    }

    private static final class InactiveStreamException extends TimeoutException {

        public InactiveStreamException(String name, Duration maxInactivity, Instant lastActive) {
            super(String.format("Stream=%s has been inactive for longer than duration=%s with lastActive=%s",
                name, maxInactivity, lastActive));
        }
    }
}
