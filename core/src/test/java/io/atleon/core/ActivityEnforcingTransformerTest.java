package io.atleon.core;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

class ActivityEnforcingTransformerTest {

    private static final Duration STEP_DURATION = Duration.ofMillis(200);

    private static final ActivityEnforcementConfig CONFIG = new ActivityEnforcementConfig(
            "test", STEP_DURATION.multipliedBy(10L), Duration.ZERO, STEP_DURATION.dividedBy(10L));

    private final Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

    private final Flux<String> downstream = sink.asFlux().transform(new ActivityEnforcingTransformer<>(CONFIG));

    @Test
    public void errorIsEmittedIfStreamIsInactive() {
        StepVerifier.create(downstream)
                .expectSubscription()
                .expectNoEvent(CONFIG.getMaxInactivity().minus(STEP_DURATION))
                .expectError(TimeoutException.class)
                .verify();
    }

    @Test
    public void errorIsEmittedIfStreamBecomesInactiveAfterEvents() {
        StepVerifier.create(downstream)
                .thenAwait(STEP_DURATION.multipliedBy(2))
                .then(() -> sink.tryEmitNext("ONE"))
                .expectNextCount(1)
                .expectNoEvent(CONFIG.getMaxInactivity().minus(STEP_DURATION))
                .expectError(TimeoutException.class)
                .verify();
    }

    @Test
    public void errorIsNotEmittedIfStreamRemainsActive() {
        StepVerifier.create(downstream)
                .thenAwait(STEP_DURATION)
                .then(() -> sink.tryEmitNext("ONE"))
                .expectNextCount(1)
                .thenAwait(STEP_DURATION)
                .then(() -> sink.tryEmitNext("TWO"))
                .expectNextCount(1)
                .expectNoEvent(CONFIG.getMaxInactivity().minus(STEP_DURATION))
                .thenCancel()
                .verify();
    }
}
