package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;

class ErrorEmitterTest {

    @Test
    public void safelyEmit_givenActiveSubscriber_expectsErrorPropagated() {
        ErrorEmitter<String> emitter = ErrorEmitter.create();
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        RuntimeException error = new RuntimeException("test error");

        Flux<String> merged = emitter.applyTo(sink.asFlux());

        StepVerifier.create(merged)
                .then(() -> {
                    sink.tryEmitNext("a");
                    emitter.safelyEmit(error);
                })
                .expectNext("a")
                .expectErrorMatches(e -> e == error)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void safelyEmit_givenSinkAlreadyTerminated_expectsNoException() {
        ErrorEmitter<String> emitter = ErrorEmitter.create(Duration.ofMillis(100));

        // First emission succeeds (even without a subscriber, the sink accepts it)
        emitter.safelyEmit(new RuntimeException("first"));

        // Second emission should not throw — failure is logged and swallowed
        emitter.safelyEmit(new RuntimeException("second"));
    }

    @Test
    public void safelyEmit_givenOngoingStream_expectsStreamTerminated() {
        ErrorEmitter<Integer> emitter = ErrorEmitter.create();
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer();
        IllegalStateException error = new IllegalStateException("stream failed");

        Flux<Integer> merged = emitter.applyTo(sink.asFlux());

        StepVerifier.create(merged)
                .then(() -> {
                    sink.tryEmitNext(1);
                    sink.tryEmitNext(2);
                })
                .expectNext(1, 2)
                .then(() -> emitter.safelyEmit(error))
                .expectErrorMatches(e ->
                        e instanceof IllegalStateException && e.getMessage().equals("stream failed"))
                .verify(Duration.ofSeconds(5));
    }
}
