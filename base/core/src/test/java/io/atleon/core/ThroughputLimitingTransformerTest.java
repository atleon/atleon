package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThroughputLimitingTransformerTest {

    @Test
    void givenUnblockedLimiter_expectsSourceElementsAndCompletion() {
        ControllableLimiter limiter = new ControllableLimiter(false);

        StepVerifier.create(Flux.just(alo(1L), alo(2L))
                        .transform(ThroughputLimitingTransformer.alo(limiter, 0))
                        .map(Alo::get))
                .expectNext(1L, 2L)
                .verifyComplete();

        assertEquals(1, limiter.mounts.get());
        assertEquals(1, limiter.unmounts.get());
        assertEquals(2, limiter.reservations.get());
    }

    @Test
    void givenBlockedLimiter_expectsSourceSubscribedOnlyAfterUnblocking() {
        ControllableLimiter limiter = new ControllableLimiter(true);
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Alo<Long>> source = Flux.just(alo(1L)).doOnSubscribe(__ -> subscriptions.incrementAndGet());

        StepVerifier.create(source.transform(ThroughputLimitingTransformer.alo(limiter, 0))
                        .map(Alo::get))
                .expectSubscription()
                .then(() -> assertEquals(0, subscriptions.get()))
                .then(() -> limiter.setBlocked(false))
                .expectNext(1L)
                .verifyComplete();

        assertEquals(1, subscriptions.get());
        assertEquals(1, limiter.unmounts.get());
    }

    @Test
    void givenLimitBecomesBlocked_expectsCancellationAndResubscription() {
        ControllableLimiter limiter = new ControllableLimiter(false);
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        Sinks.Many<Alo<Long>> elements = Sinks.many().multicast().onBackpressureBuffer(256, false);
        Flux<Alo<Long>> source = elements.asFlux()
                .doOnSubscribe(__ -> subscriptions.incrementAndGet())
                .doOnCancel(cancellations::incrementAndGet);

        StepVerifier.create(source.transform(ThroughputLimitingTransformer.alo(limiter, 0))
                        .map(Alo::get))
                .then(() -> elements.tryEmitNext(alo(1L)))
                .expectNext(1L)
                .then(() -> limiter.setBlocked(true))
                .then(() -> assertEquals(1, cancellations.get()))
                .then(() -> limiter.setBlocked(false))
                .then(() -> elements.tryEmitNext(alo(2L)))
                .expectNext(2L)
                .thenCancel()
                .verify();

        assertEquals(2, subscriptions.get());
        assertEquals(2, cancellations.get());
        assertEquals(1, limiter.unmounts.get());
    }

    @Test
    void givenRepeatedSubscription_expectsSeparateMounts() {
        ControllableLimiter limiter = new ControllableLimiter(false);
        Flux<Long> transformed = Flux.just(alo(1L))
                .transform(ThroughputLimitingTransformer.alo(limiter, 0))
                .map(Alo::get);

        StepVerifier.create(transformed.repeat(1)).expectNext(1L, 1L).verifyComplete();

        assertEquals(2, limiter.mounts.get());
        assertEquals(2, limiter.unmounts.get());
    }

    @Test
    void givenGroupedSource_expectsInnerCancellationWhenBlocked() {
        ControllableLimiter limiter = new ControllableLimiter(false);
        AtomicBoolean innerCancelled = new AtomicBoolean();
        Sinks.Many<Alo<Long>> elements = Sinks.many().multicast().onBackpressureBuffer();
        AloGroupedFlux<String, Long> group =
                new AloGroupedFlux<>(elements.asFlux().doOnCancel(() -> innerCancelled.set(true)), "group");

        StepVerifier.create(Flux.just(group)
                        .transform(ThroughputLimitingTransformer.aloGroups(limiter, 0))
                        .flatMap(AloFlux::toFlux)
                        .map(Alo::get))
                .then(() -> elements.tryEmitNext(alo(1L)))
                .expectNext(1L)
                .then(() -> limiter.setBlocked(true))
                .then(() -> assertTrue(innerCancelled.get()))
                .thenCancel()
                .verify();

        assertEquals(1, limiter.unmounts.get());
    }

    @Test
    void givenLimiterError_expectsErrorAndUnmount() {
        ControllableLimiter limiter = new ControllableLimiter(false);
        RuntimeException failure = new RuntimeException("limiter failure");

        StepVerifier.create(Flux.<Alo<Long>>never().transform(ThroughputLimitingTransformer.alo(limiter, 0)))
                .then(() -> limiter.emitError(failure))
                .expectErrorMatches(error -> error == failure)
                .verify();

        assertEquals(1, limiter.unmounts.get());
    }

    private static Alo<Long> alo(long value) {
        return new ComposedAlo<>(value, () -> {}, __ -> {});
    }

    private static final class ControllableLimiter implements ReactiveThroughputLimiter<Long> {

        private final boolean initiallyBlocked;
        private final AtomicInteger mounts = new AtomicInteger();
        private final AtomicInteger unmounts = new AtomicInteger();
        private final AtomicInteger reservations = new AtomicInteger();
        private Consumer<? super State> currentStateConsumer;
        private Consumer<? super Throwable> errorConsumer;

        private ControllableLimiter(boolean initiallyBlocked) {
            this.initiallyBlocked = initiallyBlocked;
        }

        @Override
        public Mount<Long> mount(
                Consumer<? super State> currentStateConsumer, Consumer<? super Throwable> errorConsumer) {
            mounts.incrementAndGet();
            this.currentStateConsumer = currentStateConsumer;
            this.errorConsumer = errorConsumer;
            setBlocked(initiallyBlocked);
            return new Mount<>() {
                @Override
                public Mono<?> reserveCapacity(Long value) {
                    reservations.incrementAndGet();
                    return Mono.empty();
                }

                @Override
                public void unmount() {
                    unmounts.incrementAndGet();
                }
            };
        }

        private void setBlocked(boolean blocked) {
            currentStateConsumer.accept((State) () -> blocked);
        }

        private void emitError(Throwable error) {
            errorConsumer.accept(error);
        }
    }
}
