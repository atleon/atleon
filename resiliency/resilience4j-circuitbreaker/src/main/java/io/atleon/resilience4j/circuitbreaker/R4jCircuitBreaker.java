package io.atleon.resilience4j.circuitbreaker;

import io.atleon.core.ReactiveCircuitBreaker;
import io.atleon.core.SerialQueue;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * Adapts a Resilience4j {@link CircuitBreaker} to reactive, per-element permission management.
 * OPEN and FORCED_OPEN states are reported as tripped. Permit outcomes are recorded with the
 * elapsed time since acquisition; disconnecting a permitter releases its outstanding permissions
 * without recording outcomes. Duplicate outcomes and outcomes received after release are ignored.
 */
public class R4jCircuitBreaker implements ReactiveCircuitBreaker {

    private static final Logger LOGGER = LoggerFactory.getLogger(R4jCircuitBreaker.class);

    // Weak keys allow unused breakers to be collected. ReactiveState must not store a strong
    // reference back to its breaker; mounted subscribers must still be disposed via unmount().
    private static final Map<CircuitBreaker, ReactiveState> REACTIVE_STATES =
            Collections.synchronizedMap(new WeakHashMap<>());

    private final CircuitBreaker circuitBreaker;

    private final ReactiveState reactiveState;

    private R4jCircuitBreaker(CircuitBreaker circuitBreaker, ReactiveState reactiveState) {
        this.circuitBreaker = circuitBreaker;
        this.reactiveState = reactiveState;
    }

    public static R4jCircuitBreaker create(CircuitBreaker circuitBreaker) {
        if (!circuitBreaker.getCircuitBreakerConfig().isAutomaticTransitionFromOpenToHalfOpenEnabled()) {
            throw new IllegalArgumentException("Automatic transition from OPEN to HALF_OPEN must be enabled");
        }
        var reactiveState = REACTIVE_STATES.computeIfAbsent(circuitBreaker, ReactiveState::new);
        return new R4jCircuitBreaker(circuitBreaker, reactiveState);
    }

    /**
     * Creates a mounted handle that supplies independent permitters for source connections.
     *
     * @param currentStateConsumer the consumer of breaker state notifications
     * @return a new mounted handle
     */
    @Override
    public Mount mount(Consumer<? super State> currentStateConsumer) {
        return new R4jMount(currentStateConsumer);
    }

    private static final class ReactiveState {

        private final SerialQueue<Consumer<Sinks.Many<State>>> currentStateQueue;

        private final Flux<State> currentStates;

        public ReactiveState(CircuitBreaker circuitBreaker) {
            var sink = Sinks.unsafe().many().replay().<State>latest();
            this.currentStateQueue = SerialQueue.on(sink);
            this.currentStates = sink.asFlux();
            circuitBreaker
                    .getEventPublisher()
                    .onStateTransition(__ -> emitCurrentState(circuitBreaker))
                    .onIgnoredError(__ -> emitCurrentState(circuitBreaker));
            emitCurrentState(circuitBreaker);
        }

        public Disposable subscribe(Consumer<? super State> currentStateConsumer) {
            return currentStates.subscribe(currentStateConsumer);
        }

        public void emitCurrentState(CircuitBreaker circuitBreaker) {
            currentStateQueue.addAndDrain(sink -> sink.tryEmitNext(new R4jState(circuitBreaker.getState())));
        }
    }

    private record R4jState(CircuitBreaker.State state) implements State {

        @Override
        public boolean tripped() {
            return state == CircuitBreaker.State.OPEN || state == CircuitBreaker.State.FORCED_OPEN;
        }
    }

    private final class R4jMount implements Mount {

        private final Disposable currentStateSubscription;

        public R4jMount(Consumer<? super State> currentStateConsumer) {
            this.currentStateSubscription = reactiveState.subscribe(currentStateConsumer);
        }

        @Override
        public Permitter connect() {
            return new R4jPermitter();
        }

        @Override
        public void unmount() {
            currentStateSubscription.dispose();
        }
    }

    private final class R4jPermitter implements Permitter {

        private final SerialQueue<Consumer<Set<Permit>>> inFlightPermitQueue =
                SerialQueue.on(Collections.newSetFromMap(new IdentityHashMap<>()));

        @Override
        public Optional<Permit> tryAcquirePermit() {
            if (circuitBreaker.tryAcquirePermission()) {
                InFlightPermit permit = new InFlightPermit();
                inFlightPermitQueue.addAndDrain(inFlightPermits -> inFlightPermits.add(permit));
                return Optional.of(permit);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public void disconnect() {
            inFlightPermitQueue.addAndDrain(inFlightPermits -> {
                int size = inFlightPermits.size();
                if (size > 0) {
                    IntStream.range(0, size).forEach(__ -> circuitBreaker.releasePermission());
                    inFlightPermits.clear();
                    reactiveState.emitCurrentState(circuitBreaker); // Releasing permits might have freed up capacity
                }
            });
        }

        private final class InFlightPermit implements Permit {

            private final long startNanos = System.nanoTime();

            @Override
            public void error(Throwable error) {
                settle((elapsed, timeUnit) -> circuitBreaker.onError(elapsed, timeUnit, error));
            }

            @Override
            public void success() {
                settle(circuitBreaker::onSuccess);
            }

            private void settle(BiConsumer<Long, TimeUnit> settler) {
                long elapsedNanos = System.nanoTime() - startNanos;
                inFlightPermitQueue.addAndDrain(inFlightPermits -> {
                    if (inFlightPermits.remove(this)) {
                        try {
                            settler.accept(elapsedNanos, TimeUnit.NANOSECONDS);
                        } catch (Throwable throwable) {
                            LOGGER.warn("Permit settlement failed", throwable);
                        }
                    }
                });
            }
        }
    }
}
