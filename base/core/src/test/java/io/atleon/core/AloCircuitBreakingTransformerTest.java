package io.atleon.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AloCircuitBreakingTransformerTest {

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(5);

    @Test
    public void apply_givenInitiallyTrippedBreaker_expectsSubscriptionWhenHalfOpen() {
        Fixture fixture = new Fixture();
        fixture.breaker.transition(BreakerState.OPEN);

        StepVerifier.create(fixture.flux())
                .then(() -> {
                    fixture.source.assertNoSubscribers();
                    assertEquals(1, fixture.breaker.mounts.size());
                    assertEquals(0, fixture.breaker.mounts.get(0).permitters.size());
                })
                .then(() -> fixture.breaker.transition(BreakerState.HALF_OPEN))
                .then(() -> fixture.source.assertSubscribers(1))
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(value -> assertEquals(1, value.get()))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, fixture.subscriptions.get());
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        assertEquals(1, fixture.breaker.mounts.get(0).permitters.get(0).disconnects);
    }

    @ParameterizedTest
    @EnumSource(
            value = BreakerState.class,
            names = {"CLOSED", "OPEN"})
    public void apply_givenCancellationWhileTripped_expectsMountCleanupWithoutReconnection(BreakerState initialState) {
        Fixture fixture = new Fixture();
        fixture.breaker.transition(initialState);

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.breaker.transition(BreakerState.OPEN))
                .then(() -> {
                    fixture.source.assertNoSubscribers();
                    assertEquals(0, fixture.breaker.mounts.get(0).unmounts);
                })
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        fixture.breaker.transition(BreakerState.HALF_OPEN);
        fixture.source.assertNoSubscribers();
        int expectedSourceSubscriptions = initialState == BreakerState.OPEN ? 0 : 1;
        assertEquals(expectedSourceSubscriptions, fixture.subscriptions.get());
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        assertEquals(
                expectedSourceSubscriptions,
                fixture.breaker.mounts.get(0).permitters.size());
        fixture.breaker.mounts.get(0).permitters.forEach(permitter -> assertEquals(1, permitter.disconnects));
    }

    @Test
    public void apply_givenDefaultPrefetchAndDeniedPermission_expectsOneUpstreamRequest() {
        Fixture fixture = new Fixture();
        fixture.breaker.allowed = false;

        StepVerifier.create(fixture.flux())
                .then(() -> assertEquals(1, fixture.requested.get()))
                .then(() -> fixture.source.next(alo(1)))
                .then(() -> assertEquals(1, fixture.requested.get()))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);
    }

    @ParameterizedTest
    @EnumSource(
            value = BreakerState.class,
            names = {"CLOSED", "HALF_OPEN"})
    public void apply_givenDeniedPermissions_expectsBoundedBufferingAndOrderedResumptionOnStateChange(
            BreakerState state) {
        Fixture fixture = new Fixture();
        fixture.breaker.allowed = false;

        StepVerifier.create(fixture.flux(3))
                .then(() -> assertEquals(3, fixture.requested.get()))
                .then(() -> fixture.source.next(alo(1), alo(2), alo(3)))
                .then(() -> assertEquals(3, fixture.requested.get()))
                .then(() -> {
                    fixture.breaker.allowed = true;
                    fixture.breaker.transition(state);
                })
                .assertNext(value -> assertEquals(1, value.get()))
                .assertNext(value -> assertEquals(2, value.get()))
                .assertNext(value -> assertEquals(3, value.get()))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, fixture.subscriptions.get());
    }

    @Test
    public void apply_givenLimitedDownstreamDemand_expectsDeliveryWithinDemand() {
        Fixture fixture = new Fixture();

        StepVerifier.create(fixture.flux(3), 0)
                .then(() -> assertEquals(0, fixture.requested.get()))
                .thenRequest(1)
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(value -> assertEquals(1, value.get()))
                .then(() -> fixture.source.assertMinRequested(0).assertMaxRequested(0))
                .thenRequest(2)
                .then(() -> fixture.source.next(alo(2), alo(3)))
                .assertNext(value -> assertEquals(2, value.get()))
                .assertNext(value -> assertEquals(3, value.get()))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    public void apply_givenConnectedStateChanges_expectsExistingSourceSubscription() {
        Fixture fixture = new Fixture();

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.breaker.transition(BreakerState.HALF_OPEN))
                .then(() -> fixture.breaker.transition(BreakerState.CLOSED))
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(value -> assertEquals(1, value.get()))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, fixture.subscriptions.get());
        assertEquals(1, fixture.cancellations.get());
        assertEquals(1, fixture.breaker.mounts.get(0).permitters.size());
    }

    @Test
    public void apply_givenAcknowledgement_expectsPermitSuccessAndOriginalAcknowledgement() {
        Fixture fixture = new Fixture();
        AtomicInteger acknowledged = new AtomicInteger();

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.source.next(new ComposedAlo<>(1, acknowledged::incrementAndGet, error -> {})))
                .assertNext(value -> {
                    Alo.acknowledge(value);
                    assertEquals(1, fixture.breaker.permits.get(0).successes);
                    assertEquals(1, acknowledged.get());
                })
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        assertEquals(0, fixture.breaker.permits.get(0).errors);
    }

    @Test
    public void apply_givenNegativeAcknowledgement_expectsOriginalErrorReportedToPermitAndNacknowledger() {
        Fixture fixture = new Fixture();
        RuntimeException failure = new IllegalStateException("processing failed");
        AtomicReference<Throwable> nacknowledged = new AtomicReference<>();

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.source.next(new ComposedAlo<>(1, () -> {}, nacknowledged::set)))
                .assertNext(value -> Alo.nacknowledge(value, failure))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        TestPermit permit = fixture.breaker.permits.get(0);
        assertEquals(1, permit.errors);
        assertSame(failure, permit.failure.get());
        assertSame(failure, nacknowledged.get());
        assertEquals(0, permit.successes);
    }

    @Test
    public void apply_givenBreakerOpensThenBecomesHalfOpen_expectsCancellationDisconnectionAndResubscription() {
        Fixture fixture = new Fixture();
        AtomicReference<Alo<Integer>> first = new AtomicReference<>();
        AtomicInteger acknowledged = new AtomicInteger();

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.source.next(new ComposedAlo<>(1, acknowledged::incrementAndGet, error -> {})))
                .assertNext(first::set)
                .then(() -> fixture.breaker.transition(BreakerState.OPEN))
                .then(() -> {
                    fixture.source.assertNoSubscribers();
                    assertEquals(1, fixture.cancellations.get());
                    assertEquals(0, fixture.breaker.mounts.get(0).unmounts);
                    assertEquals(1, fixture.breaker.mounts.get(0).permitters.get(0).disconnects);
                    Alo.acknowledge(first.get());
                    assertEquals(1, acknowledged.get());
                    assertEquals(1, fixture.breaker.permits.get(0).successes);
                })
                .then(() -> fixture.breaker.transition(BreakerState.OPEN))
                .then(() -> assertEquals(1, fixture.subscriptions.get()))
                .then(() -> fixture.breaker.transition(BreakerState.HALF_OPEN))
                .then(() -> fixture.source.next(alo(2)))
                .assertNext(value -> assertEquals(2, value.get()))
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        assertEquals(2, fixture.subscriptions.get());
        assertEquals(2, fixture.cancellations.get());
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        List<TestBreaker.TestPermitter> permitters = fixture.breaker.mounts.get(0).permitters;
        assertEquals(2, permitters.size());
        assertEquals(1, permitters.get(0).acquiredPermits.get());
        assertEquals(1, permitters.get(1).acquiredPermits.get());
        permitters.forEach(permitter -> assertEquals(1, permitter.disconnects));
    }

    @Test
    public void apply_givenReusedPermitter_expectsSeparateConnectionAndMountCleanup() {
        Fixture fixture = new Fixture();
        AtomicReference<Consumer<? super ReactiveCircuitBreaker.State>> stateConsumer = new AtomicReference<>();
        AtomicInteger connections = new AtomicInteger();
        AtomicInteger disconnects = new AtomicInteger();
        AtomicInteger unmounts = new AtomicInteger();
        AtomicInteger acquiredPermits = new AtomicInteger();
        ReactiveCircuitBreaker.Permitter permitter = new ReactiveCircuitBreaker.Permitter() {
            @Override
            public Optional<ReactiveCircuitBreaker.Permit> tryAcquirePermit() {
                acquiredPermits.incrementAndGet();
                return Optional.of(new TestPermit());
            }

            @Override
            public void disconnect() {
                disconnects.incrementAndGet();
            }
        };
        ReactiveCircuitBreaker breaker = consumer -> {
            stateConsumer.set(consumer);
            consumer.accept(BreakerState.CLOSED);
            return new ReactiveCircuitBreaker.Mounted() {
                @Override
                public ReactiveCircuitBreaker.Permitter connect() {
                    connections.incrementAndGet();
                    return permitter;
                }

                @Override
                public void unmount() {
                    unmounts.incrementAndGet();
                }
            };
        };

        StepVerifier.create(fixture.sourceFlux().transform(AloCircuitBreakingTransformer.create(breaker)))
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(Alo::acknowledge)
                .then(() -> stateConsumer.get().accept(BreakerState.OPEN))
                .then(() -> {
                    fixture.source.assertNoSubscribers();
                    assertEquals(1, connections.get());
                    assertEquals(1, disconnects.get());
                    assertEquals(0, unmounts.get());
                })
                .then(() -> stateConsumer.get().accept(BreakerState.HALF_OPEN))
                .then(() -> fixture.source.next(alo(2)))
                .assertNext(Alo::acknowledge)
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        assertEquals(2, acquiredPermits.get());
        assertEquals(2, fixture.subscriptions.get());
        assertEquals(2, connections.get());
        assertEquals(2, disconnects.get());
        assertEquals(1, unmounts.get());
    }

    @Test
    public void apply_givenDownstreamCancellation_expectsDisconnectionAndForwardedLateOutcomes() {
        Fixture fixture = new Fixture();
        List<Alo<Integer>> received = new ArrayList<>();
        AtomicInteger acknowledged = new AtomicInteger();
        AtomicInteger nacknowledged = new AtomicInteger();

        StepVerifier.create(fixture.flux(3))
                .then(() -> fixture.source.next(
                        alo(1),
                        new ComposedAlo<>(2, acknowledged::incrementAndGet, error -> {}),
                        new ComposedAlo<>(3, () -> {}, error -> nacknowledged.incrementAndGet())))
                .assertNext(Alo::acknowledge)
                .assertNext(received::add)
                .assertNext(received::add)
                .thenCancel()
                .verify(VERIFY_TIMEOUT);

        Alo.acknowledge(received.get(0));
        RuntimeException failure = new IllegalStateException("late failure");
        Alo.nacknowledge(received.get(1), failure);
        assertEquals(1, acknowledged.get());
        assertEquals(1, nacknowledged.get());
        assertEquals(1, fixture.breaker.permits.get(0).successes);
        assertEquals(0, fixture.breaker.permits.get(0).errors);
        assertEquals(1, fixture.breaker.permits.get(1).successes);
        assertEquals(0, fixture.breaker.permits.get(1).errors);
        assertEquals(0, fixture.breaker.permits.get(2).successes);
        assertEquals(1, fixture.breaker.permits.get(2).errors);
        assertSame(failure, fixture.breaker.permits.get(2).failure.get());
        assertEquals(1, fixture.cancellations.get());
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        assertEquals(1, fixture.breaker.mounts.get(0).permitters.get(0).disconnects);
    }

    @Test
    public void apply_givenSourceErrorWithDeniedBufferedItem_expectsImmediateErrorAndDisconnection() {
        Fixture fixture = new Fixture();
        RuntimeException failure = new IllegalStateException("source failed");

        StepVerifier.create(fixture.flux(2))
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(value -> assertEquals(1, value.get()))
                .then(() -> fixture.breaker.allowed = false)
                .then(() -> fixture.source.next(alo(2)))
                .then(() -> fixture.source.error(failure))
                .expectErrorMatches(error -> error == failure)
                .verify(VERIFY_TIMEOUT);

        assertEquals(0, fixture.breaker.permits.get(0).successes);
        assertEquals(0, fixture.breaker.permits.get(0).errors);
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        assertEquals(1, fixture.breaker.mounts.get(0).permitters.get(0).disconnects);
    }

    @Test
    public void apply_givenPermissionAcquirementFailure_expectsErrorAndSourceCancellation() {
        Fixture fixture = new Fixture();
        RuntimeException failure = new IllegalStateException("permission failed");
        fixture.breaker.acquirementFailure = Optional.of(failure);

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.source.next(alo(1)))
                .expectErrorMatches(error -> error == failure)
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, fixture.cancellations.get());
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        assertEquals(1, fixture.breaker.mounts.get(0).permitters.get(0).disconnects);
    }

    @Test
    public void apply_givenSourceCompletion_expectsCompletionWithoutFurtherResubscription() {
        Fixture fixture = new Fixture();

        StepVerifier.create(fixture.flux())
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(Alo::acknowledge)
                .then(fixture.source::complete)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        fixture.breaker.transition(BreakerState.OPEN);
        fixture.breaker.transition(BreakerState.HALF_OPEN);
        fixture.source.assertNoSubscribers();
        assertEquals(1, fixture.subscriptions.get());
        assertEquals(1, fixture.breaker.mounts.size());
        assertEquals(1, fixture.breaker.mounts.get(0).unmounts);
        assertEquals(1, fixture.breaker.mounts.get(0).permitters.get(0).disconnects);
    }

    @Test
    public void apply_givenFinalAcknowledgementTripsBreakerDuringRecovery_expectsCompletionAndCleanup() {
        TestBreaker breaker = new TestBreaker();
        breaker.transition(BreakerState.OPEN);
        AtomicInteger subscriptions = new AtomicInteger();
        Alo<Integer> value = new ComposedAlo<>(1, () -> breaker.transition(BreakerState.OPEN), error -> {});
        Flux<Alo<Integer>> flux = Flux.just(value)
                .doOnSubscribe(__ -> subscriptions.incrementAndGet())
                .transform(AloCircuitBreakingTransformer.create(breaker))
                .doOnNext(Alo::acknowledge);

        StepVerifier.create(flux)
                .then(() -> assertEquals(0, subscriptions.get()))
                // Recover while state notifications are being drained so the final acknowledgement
                // queues OPEN before the synchronous source queues completion.
                .then(() -> breaker.transition(BreakerState.HALF_OPEN))
                .assertNext(alo -> assertEquals(1, alo.get()))
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, breaker.mounts.size());
        assertEquals(1, breaker.mounts.get(0).unmounts);
        assertEquals(1, breaker.mounts.get(0).permitters.size());
        assertEquals(1, breaker.mounts.get(0).permitters.get(0).disconnects);
        breaker.transition(BreakerState.HALF_OPEN);
        assertEquals(1, subscriptions.get());
    }

    @Test
    public void apply_givenMultipleDownstreamSubscriptions_expectsIndependentBreakerMounts() {
        Fixture fixture = new Fixture();
        Flux<Alo<Integer>> flux = fixture.flux();
        assertEquals(0, fixture.breaker.mounts.size());

        StepVerifier.create(Flux.merge(flux.take(1), flux.take(1)))
                .then(() -> assertEquals(2, fixture.breaker.mounts.size()))
                .then(() -> fixture.source.next(alo(1)))
                .assertNext(value -> assertEquals(1, value.get()))
                .assertNext(value -> assertEquals(1, value.get()))
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        fixture.breaker.transition(BreakerState.OPEN);
        fixture.breaker.transition(BreakerState.HALF_OPEN);
        fixture.source.assertNoSubscribers();
        assertEquals(2, fixture.subscriptions.get());
        assertEquals(2, fixture.breaker.permits.size());
        fixture.breaker.mounts.forEach(mounted -> {
            assertEquals(1, mounted.unmounts);
            assertEquals(1, mounted.permitters.size());
            assertEquals(1, mounted.permitters.get(0).acquiredPermits.get());
            assertEquals(1, mounted.permitters.get(0).disconnects);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    public void withMaxPrefetch_givenNonPositivePrefetch_expectsIllegalArgumentException(int maxPrefetch) {
        assertThrows(IllegalArgumentException.class, () -> AloCircuitBreakingTransformer.create(new TestBreaker())
                .withMaxPrefetch(maxPrefetch));
    }

    private static Alo<Integer> alo(int value) {
        return new ComposedAlo<>(value, () -> {}, error -> {});
    }

    private enum BreakerState implements ReactiveCircuitBreaker.State {
        CLOSED,
        HALF_OPEN,
        OPEN;

        @Override
        public boolean tripped() {
            return this == OPEN;
        }
    }

    private static final class Fixture {

        private final TestPublisher<Alo<Integer>> source = TestPublisher.create();

        private final TestBreaker breaker = new TestBreaker();

        private final AtomicInteger subscriptions = new AtomicInteger();

        private final AtomicInteger cancellations = new AtomicInteger();

        private final AtomicLong requested = new AtomicLong();

        private Flux<Alo<Integer>> flux() {
            return sourceFlux().transform(AloCircuitBreakingTransformer.create(breaker));
        }

        private Flux<Alo<Integer>> flux(int maxPrefetch) {
            return sourceFlux()
                    .transform(AloCircuitBreakingTransformer.<Integer>create(breaker)
                            .withMaxPrefetch(maxPrefetch));
        }

        private Flux<Alo<Integer>> sourceFlux() {
            return source.flux()
                    .doOnSubscribe(subscription -> subscriptions.incrementAndGet())
                    .doOnCancel(cancellations::incrementAndGet)
                    .doOnRequest(requested::addAndGet);
        }
    }

    private static final class TestBreaker implements ReactiveCircuitBreaker {

        private final List<TestMounted> mounts = new ArrayList<>();

        private State state = BreakerState.CLOSED;

        private final List<TestPermit> permits = new ArrayList<>();

        private boolean allowed = true;

        private Optional<RuntimeException> acquirementFailure = Optional.empty();

        @Override
        public Mounted mount(Consumer<? super State> currentStateConsumer) {
            TestMounted mounted = new TestMounted(currentStateConsumer);
            mounts.add(mounted);
            currentStateConsumer.accept(state);
            return mounted;
        }

        private Optional<Permit> tryAcquirePermit() {
            acquirementFailure.ifPresent(failure -> {
                throw failure;
            });
            if (!allowed) {
                return Optional.empty();
            }
            TestPermit permit = new TestPermit();
            permits.add(permit);
            return Optional.of(permit);
        }

        private void transition(BreakerState state) {
            this.state = state;
            mounts.stream()
                    .filter(mounted -> mounted.unmounts == 0)
                    .forEach(mounted -> mounted.currentStateConsumer.accept(state));
        }

        private final class TestMounted implements Mounted {

            private final Consumer<? super State> currentStateConsumer;

            private final List<TestPermitter> permitters = new ArrayList<>();

            private int unmounts;

            private TestMounted(Consumer<? super State> currentStateConsumer) {
                this.currentStateConsumer = currentStateConsumer;
            }

            @Override
            public Permitter connect() {
                assertEquals(0, unmounts);
                TestPermitter permitter = new TestPermitter();
                permitters.add(permitter);
                return permitter;
            }

            @Override
            public void unmount() {
                unmounts++;
            }
        }

        private final class TestPermitter implements Permitter {

            private final AtomicInteger acquiredPermits = new AtomicInteger();

            private int disconnects;

            @Override
            public Optional<Permit> tryAcquirePermit() {
                Optional<Permit> permit = TestBreaker.this.tryAcquirePermit();
                permit.ifPresent(__ -> acquiredPermits.incrementAndGet());
                return permit;
            }

            @Override
            public void disconnect() {
                disconnects++;
            }
        }
    }

    private static final class TestPermit implements ReactiveCircuitBreaker.Permit {

        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private int successes;

        private int errors;

        @Override
        public void success() {
            successes++;
        }

        @Override
        public void error(Throwable error) {
            errors++;
            failure.set(error);
        }
    }
}
