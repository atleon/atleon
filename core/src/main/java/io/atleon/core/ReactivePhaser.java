package io.atleon.core;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
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
        return arriveAndAwaitAdvanceReactively(Schedulers.parallel());
    }

    public Mono<Integer> arriveAndAwaitAdvanceReactively(Scheduler scheduler) {
        return Mono.fromSupplier(this::arrive)
            .flatMap(arrivalPhase -> awaitAdvanceReactively(arrivalPhase, scheduler).thenReturn(arrivalPhase))
            .cache();
    }

    public Mono<Integer> awaitAdvanceReactively(int phase) {
        return awaitAdvanceReactively(phase, Schedulers.parallel());
    }

    public Mono<Integer> awaitAdvanceReactively(int phase, Scheduler scheduler) {
        if (phase < 0) {
            return Mono.just(phase);
        } else {
            return sink.asFlux()
                .publishOn(scheduler)
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
