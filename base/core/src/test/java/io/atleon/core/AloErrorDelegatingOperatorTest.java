package io.atleon.core;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloErrorDelegatingOperatorTest {

    @Test
    public void acknowledgementIsExecutedIfErrorIsSuccessfullyDelegated() {
        TestAlo alo = new TestAlo("DATA");

        new AloErrorDelegatingOperator<>(Flux.just(alo), (data, error) -> Flux.just(data))
                .doOnNext(it -> Alo.nacknowledge(it, new UnsupportedOperationException("Fail")))
                .blockLast();

        assertTrue(alo.isAcknowledged());
        assertFalse(alo.isNacknowledged());
    }

    @Test
    public void delegationIsCanceledIfUpstreamErrorsWhileInFlight() {
        TestAlo alo = new TestAlo("DATA");

        AtomicBoolean delegated = new AtomicBoolean(false);
        AtomicBoolean canceled = new AtomicBoolean(false);
        BiFunction<String, Throwable, Publisher<?>> delegator = (data, error) -> Flux.just(data)
                .delayElements(Duration.ofSeconds(5))
                .doAfterTerminate(() -> delegated.set(true))
                .doOnCancel(() -> canceled.set(true));

        Flux<Alo<String>> flux = Flux.concat(Flux.just(alo), Flux.error(new UnsupportedOperationException("Boom")));
        new AloErrorDelegatingOperator<>(flux, delegator)
                .doOnNext(it -> Alo.nacknowledge(it, new UnsupportedOperationException("Fail")))
                .onErrorComplete()
                .blockLast();

        Timing.waitForCondition(canceled::get);
        assertFalse(alo.isAcknowledged());
        assertTrue(alo.getError().map(CancellationException.class::isInstance).orElse(false));
        assertTrue(canceled.get());
        assertFalse(delegated.get());
    }

    @Test
    public void delegationIsCanceledIfDownstreamCancelsWhileInFlight() {
        TestAlo alo = new TestAlo("DATA");

        AtomicBoolean delegated = new AtomicBoolean(false);
        AtomicBoolean canceled = new AtomicBoolean(false);
        BiFunction<String, Throwable, Publisher<?>> delegator = (data, error) -> Flux.just(data)
                .delayElements(Duration.ofSeconds(5))
                .doAfterTerminate(() -> delegated.set(true))
                .doOnCancel(() -> canceled.set(true));

        Flux<Alo<String>> flux = Flux.concat(Flux.just(alo), Flux.never());
        new AloErrorDelegatingOperator<>(flux, delegator)
                .doOnNext(it -> Alo.nacknowledge(it, new UnsupportedOperationException("Fail")))
                .take(Duration.ofSeconds(1))
                .blockLast();

        Timing.waitForCondition(canceled::get);
        assertFalse(alo.isAcknowledged());
        assertTrue(alo.getError().map(CancellationException.class::isInstance).orElse(false));
        assertTrue(canceled.get());
        assertFalse(delegated.get());
    }
}
