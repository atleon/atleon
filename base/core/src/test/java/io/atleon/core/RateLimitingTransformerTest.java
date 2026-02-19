package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

class RateLimitingTransformerTest {

    private static final Duration PERMIT_DURATION = Duration.ofMillis(500L); // 2 Permits per Second

    private static final Duration STEP_DURATION = PERMIT_DURATION.dividedBy(5L); // 5 "steps" per Permit

    private static final double PERMITS_PER_SECOND =
            Duration.ofMillis(1000L).dividedBy(PERMIT_DURATION.toMillis()).toMillis();

    private final RateLimitingConfig config = new RateLimitingConfig(PERMITS_PER_SECOND);

    @Test
    public void publishersCanBeRateLimited() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        sink.asFlux()
                .publishOn(Schedulers.single())
                .transform(new RateLimitingTransformer<>(config, Schedulers.boundedElastic()))
                .as(StepVerifier::create)
                .then(() -> {
                    sink.tryEmitNext("ONE");
                    sink.tryEmitNext("TWO");
                    sink.tryEmitNext("THREE");
                })
                .expectNext("ONE")
                .expectNoEvent(PERMIT_DURATION.minus(STEP_DURATION))
                .expectNext("TWO")
                .expectNoEvent(PERMIT_DURATION.minus(STEP_DURATION))
                .expectNext("THREE")
                .thenCancel()
                .verify();
    }
}
