package io.atleon.resilience4j.circuitbreaker;

import io.atleon.core.ReactiveCircuitBreaker;
import io.atleon.core.SerialQueue;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
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
import java.util.concurrent.ConcurrentHashMap;
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

    private static final Set<CircuitBreakerRegistry> ATTACHED_REGISTRIES = ConcurrentHashMap.newKeySet();

    private static final Map<CircuitBreaker, R4jCircuitBreaker> CANONICAL_POOL = new ConcurrentHashMap<>();

    private final SerialQueue<Consumer<Sinks.Many<State>>> currentStateQueue;

    private final Flux<State> currentStates;

    private final CircuitBreaker circuitBreaker;

    private R4jCircuitBreaker(CircuitBreaker circuitBreaker) {
        var sink = Sinks.unsafe().many().replay().<State>latest();
        this.currentStateQueue = SerialQueue.on(sink);
        this.currentStates = sink.asFlux();
        this.circuitBreaker = circuitBreaker;
        circuitBreaker
                .getEventPublisher()
                .onStateTransition(__ -> emitCurrentState())
                .onIgnoredError(__ -> emitCurrentState());
        emitCurrentState();
    }

    /**
     * Returns the shared adapter for a breaker in the supplied registry. Automatic transition
     * from OPEN to HALF_OPEN is required so suspended processing can resume without requesting
     * permission. The adapter is removed from the pool when the registry entry is removed or
     * replaced.
     *
     * @param registry the registry containing the breaker
     * @param name the breaker's name
     * @return the existing or newly registered adapter
     * @throws IllegalArgumentException if automatic transition from OPEN to HALF_OPEN is disabled
     */
    public static R4jCircuitBreaker register(CircuitBreakerRegistry registry, String name) {
        CircuitBreaker circuitBreaker = registry.circuitBreaker(name);
        if (!circuitBreaker.getCircuitBreakerConfig().isAutomaticTransitionFromOpenToHalfOpenEnabled()) {
            throw new IllegalArgumentException("Automatic transition from OPEN to HALF_OPEN must be enabled");
        }
        if (ATTACHED_REGISTRIES.add(registry)) {
            registry.getEventPublisher()
                    .onEntryReplaced(event -> CANONICAL_POOL.remove(event.getOldEntry()))
                    .onEntryRemoved(event -> CANONICAL_POOL.remove(event.getRemovedEntry()));
        }
        // Known race condition here: The CircuitBreaker being registered could be removed/replaced
        // in the now-attached registry prior to completion of canonicalization. That could cause
        // us to canonicalize a CircuitBreaker that is no longer used in any registry. There is not
        // much we can do about it since Resilience4j's API does not support removing listeners
        // after adding them, which is the only reason we need attachment and canonicalization.
        // Otherwise, we'd be at risk of registering an unbounded number of listeners.
        return CANONICAL_POOL.computeIfAbsent(circuitBreaker, R4jCircuitBreaker::new);
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

    private void emitCurrentState() {
        currentStateQueue.addAndDrain(sink -> sink.tryEmitNext(new R4jState(circuitBreaker.getState())));
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
            this.currentStateSubscription = currentStates.subscribe(currentStateConsumer);
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
                    emitCurrentState(); // Releasing permits might have freed up capacity
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
