package io.atleon.core;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Phaser;

/**
 * An extension of {@link Phaser} that allows subscribing to phase advances.
 */
public final class ReactivePhaser extends Phaser {

    private final Sinks.Many<Integer> sink = Sinks.many().replay().latest();

    public ReactivePhaser(int parties) {
        super(parties);
    }

    @Override
    public void forceTermination() {
        super.forceTermination();
        sink.tryEmitNext(getPhase());
    }

    public Mono<Integer> arriveAndAwaitAdvanceReactively() {
        return Mono.fromSupplier(this::arrive)
            .flatMap(arrivalPhase -> awaitAdvanceReactively(arrivalPhase).thenReturn(arrivalPhase))
            .cache();
    }

    public Mono<Integer> awaitAdvanceReactively(int phase) {
        if (phase < 0) {
            return Mono.just(phase);
        } else {
            return sink.asFlux()
                .publishOn(Schedulers.parallel())
                .filter(phaseAdvancedTo -> phaseAdvancedTo < 0 || phaseAdvancedTo > phase)
                .next();
        }
    }

    @Override
    protected boolean onAdvance(int phase, int registeredParties) {
        boolean shouldTerminate = super.onAdvance(phase, registeredParties);
        sink.tryEmitNext(shouldTerminate ? (Integer.MIN_VALUE + phase) : ((phase + 1) & Integer.MAX_VALUE));
        return shouldTerminate;
    }
}
