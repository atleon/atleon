package io.atleon.resilience4j.circuitbreaker;

import io.atleon.core.ReactiveCircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R4jCircuitBreakerTest {

    private final List<ReactiveCircuitBreaker.Mount> mounts = new ArrayList<>();

    private final List<ReactiveCircuitBreaker.Permitter> connections = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        mounts.forEach(ReactiveCircuitBreaker.Mount::unmount);
        connections.forEach(ReactiveCircuitBreaker.Permitter::disconnect);
    }

    @Test
    public void create_givenAutomaticTransitionDisabled_expectsRejection() {
        CircuitBreaker breaker = newBreaker(CircuitBreakerConfig.ofDefaults());

        assertThrows(IllegalArgumentException.class, () -> R4jCircuitBreaker.create(breaker));
    }

    @Test
    public void create_givenSameBreaker_expectsDistinctAdapters() {
        CircuitBreaker breaker = newBreaker();

        assertNotSame(R4jCircuitBreaker.create(breaker), R4jCircuitBreaker.create(breaker));
    }

    @Test
    public void create_givenDistinctBreakersWithSameName_expectsIndependentStateAndPermissions() {
        CircuitBreaker first = newBreaker();
        CircuitBreaker second = newBreaker();
        List<Boolean> firstStates = new ArrayList<>();
        List<Boolean> secondStates = new ArrayList<>();
        ReactiveCircuitBreaker.Permitter firstPermitter =
                connect(mount(first, state -> firstStates.add(state.tripped())));
        ReactiveCircuitBreaker.Permitter secondPermitter =
                connect(mount(second, state -> secondStates.add(state.tripped())));

        first.transitionToOpenState();

        assertEquals(List.of(false, true), firstStates);
        assertEquals(List.of(false), secondStates);
        assertTrue(firstPermitter.tryAcquirePermit().isEmpty());
        assertTrue(secondPermitter.tryAcquirePermit().isPresent());
    }

    @Test
    public void mount_givenClosedBreaker_expectsImmediateNonTrippedState() {
        List<Boolean> states = new ArrayList<>();

        mount(newBreaker(), state -> states.add(state.tripped()));

        assertEquals(List.of(false), states);
    }

    @Test
    public void mount_givenAlreadyOpenBreaker_expectsImmediateTrippedState() {
        CircuitBreaker breaker = newBreaker();
        breaker.transitionToOpenState();
        List<Boolean> states = new ArrayList<>();

        mount(breaker, state -> states.add(state.tripped()));

        assertEquals(List.of(true), states);
    }

    @Test
    public void mount_givenStateTransitions_expectsOnlyOpenAndForcedOpenToBeTripped() {
        CircuitBreaker breaker = newBreaker();
        List<Boolean> states = new ArrayList<>();
        mount(breaker, state -> states.add(state.tripped()));

        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
        breaker.transitionToClosedState();
        breaker.transitionToForcedOpenState();
        breaker.transitionToDisabledState();
        breaker.transitionToMetricsOnlyState();

        assertEquals(List.of(false, true, false, false, true, false, false), states);
    }

    @Test
    public void mount_givenExistingMountAndPriorTransitions_expectsLatestStateWithoutHistory() {
        CircuitBreaker breaker = newBreaker();
        List<Boolean> existingStates = new ArrayList<>();
        mount(breaker, state -> existingStates.add(state.tripped()));
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
        List<Boolean> newStates = new ArrayList<>();

        mount(breaker, state -> newStates.add(state.tripped()));
        breaker.transitionToOpenState();

        assertEquals(List.of(false, true, false, true), existingStates);
        assertEquals(List.of(false, true), newStates);
    }

    @Test
    public void unmount_givenMultipleMounts_expectsOnlyUnmountedConsumerToStopReceivingStates() {
        CircuitBreaker breaker = newBreaker();
        List<Boolean> firstStates = new ArrayList<>();
        List<Boolean> secondStates = new ArrayList<>();
        ReactiveCircuitBreaker.Mount first = mount(breaker, state -> firstStates.add(state.tripped()));
        mount(breaker, state -> secondStates.add(state.tripped()));

        first.unmount();
        first.unmount();
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();

        assertEquals(List.of(false), firstStates);
        assertEquals(List.of(false, true, false), secondStates);
    }

    @Test
    public void connect_givenDisconnectedSource_expectsNewPermitterAndContinuedStateNotifications() {
        CircuitBreaker breaker = newBreaker();
        List<Boolean> states = new ArrayList<>();
        ReactiveCircuitBreaker.Mount mount = mount(breaker, state -> states.add(state.tripped()));
        ReactiveCircuitBreaker.Permitter first = connect(mount);

        first.disconnect();
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
        ReactiveCircuitBreaker.Permitter second = connect(mount);

        assertNotSame(first, second);
        assertTrue(second.tryAcquirePermit().isPresent());
        assertEquals(List.of(false, true, false), states);
    }

    @Test
    public void tryAcquirePermit_givenClosedBreaker_expectsPermitWithoutRecordingOutcome() {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, __ -> {}));

        assertTrue(permitter.tryAcquirePermit().isPresent());
        assertEquals(0, breaker.getMetrics().getNumberOfBufferedCalls());
    }

    @Test
    public void tryAcquirePermit_givenOpenOrForcedOpenBreaker_expectsDenial() {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, __ -> {}));

        breaker.transitionToOpenState();
        assertTrue(permitter.tryAcquirePermit().isEmpty());
        breaker.transitionToForcedOpenState();
        assertTrue(permitter.tryAcquirePermit().isEmpty());
        assertEquals(0, breaker.getMetrics().getNumberOfBufferedCalls());
    }

    @Test
    public void tryAcquirePermit_givenHalfOpenConnectionsAcrossMounts_expectsSharedProbeLimit() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        ReactiveCircuitBreaker.Mount firstMount = mount(breaker, __ -> {});
        ReactiveCircuitBreaker.Permitter first = connect(firstMount);
        ReactiveCircuitBreaker.Permitter second = connect(firstMount);
        ReactiveCircuitBreaker.Permitter third = connect(mount(breaker, __ -> {}));

        assertTrue(first.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(first.tryAcquirePermit().isEmpty());
        assertTrue(third.tryAcquirePermit().isEmpty());
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
    }

    @Test
    public void success_givenAcquiredPermit_expectsSuccessfulOutcomeRecorded() {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permit permit =
                connect(mount(breaker, __ -> {})).tryAcquirePermit().orElseThrow();

        permit.success();

        assertEquals(1, breaker.getMetrics().getNumberOfSuccessfulCalls());
        assertEquals(0, breaker.getMetrics().getNumberOfFailedCalls());
    }

    @Test
    public void error_givenAcquiredPermit_expectsOriginalFailureRecorded() {
        CircuitBreaker breaker = newBreaker();
        List<Throwable> errors = new ArrayList<>();
        breaker.getEventPublisher().onError(event -> errors.add(event.getThrowable()));
        ReactiveCircuitBreaker.Permit permit =
                connect(mount(breaker, __ -> {})).tryAcquirePermit().orElseThrow();
        RuntimeException failure = new RuntimeException("processing failed");

        permit.error(failure);

        assertEquals(List.of(failure), errors);
        assertEquals(1, breaker.getMetrics().getNumberOfFailedCalls());
        assertEquals(0, breaker.getMetrics().getNumberOfSuccessfulCalls());
    }

    @Test
    public void error_givenFailureThresholdReached_expectsTrippedNotification() {
        CircuitBreaker breaker = newBreaker();
        List<Boolean> states = new ArrayList<>();
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, state -> states.add(state.tripped())));

        permitter.tryAcquirePermit().orElseThrow().error(new RuntimeException("first"));
        permitter.tryAcquirePermit().orElseThrow().error(new RuntimeException("second"));

        assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
        assertEquals(List.of(false, true), states);
        assertTrue(permitter.tryAcquirePermit().isEmpty());
    }

    @Test
    public void success_givenAllHalfOpenProbesSucceed_expectsClosedNotification() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        List<Boolean> states = new ArrayList<>();
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, state -> states.add(state.tripped())));
        ReactiveCircuitBreaker.Permit first = permitter.tryAcquirePermit().orElseThrow();
        ReactiveCircuitBreaker.Permit second = permitter.tryAcquirePermit().orElseThrow();

        first.success();
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
        second.success();

        assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
        assertEquals(List.of(false, false), states);
    }

    @Test
    public void error_givenIgnoredHalfOpenFailure_expectsRetryNotificationAndRestoredCapacity() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        List<Boolean> states = new ArrayList<>();
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, state -> states.add(state.tripped())));
        ReactiveCircuitBreaker.Permit first = permitter.tryAcquirePermit().orElseThrow();
        assertTrue(permitter.tryAcquirePermit().isPresent());
        assertTrue(permitter.tryAcquirePermit().isEmpty());

        first.error(new IllegalArgumentException("ignored"));

        assertEquals(List.of(false, false), states);
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
        assertEquals(0, breaker.getMetrics().getNumberOfBufferedCalls());
        assertTrue(permitter.tryAcquirePermit().isPresent());
        assertTrue(permitter.tryAcquirePermit().isEmpty());
    }

    @Test
    public void success_givenRepeatedAndConflictingCallbacks_expectsOnlyFirstOutcomeRecorded() {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permit permit =
                connect(mount(breaker, __ -> {})).tryAcquirePermit().orElseThrow();

        permit.success();
        permit.success();
        permit.error(new RuntimeException("late failure"));

        assertEquals(1, breaker.getMetrics().getNumberOfSuccessfulCalls());
        assertEquals(0, breaker.getMetrics().getNumberOfFailedCalls());
    }

    @Test
    public void error_givenRepeatedAndConflictingCallbacks_expectsOnlyFirstOutcomeRecorded() {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permit permit =
                connect(mount(breaker, __ -> {})).tryAcquirePermit().orElseThrow();

        permit.error(new RuntimeException("first failure"));
        permit.error(new RuntimeException("duplicate failure"));
        permit.success();

        assertEquals(1, breaker.getMetrics().getNumberOfFailedCalls());
        assertEquals(0, breaker.getMetrics().getNumberOfSuccessfulCalls());
    }

    @Test
    public void error_givenThrowingOutcomeRecorder_expectsNoExceptionAndSubsequentPermitsToSettle() {
        CircuitBreaker breaker = newBreaker(config().recordException(error -> {
                    throw new IllegalStateException("recorder failed");
                })
                .build());
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, __ -> {}));
        ReactiveCircuitBreaker.Permit failing = permitter.tryAcquirePermit().orElseThrow();

        assertDoesNotThrow(() -> failing.error(new RuntimeException("processing failed")));
        assertDoesNotThrow(failing::success);
        permitter.tryAcquirePermit().orElseThrow().success();

        assertEquals(1, breaker.getMetrics().getNumberOfSuccessfulCalls());
    }

    @Test
    public void disconnect_givenOutstandingHalfOpenPermits_expectsReleaseAndRetryNotificationWithoutOutcomes() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        List<Boolean> states = new ArrayList<>();
        ReactiveCircuitBreaker.Mount mount = mount(breaker, state -> states.add(state.tripped()));
        ReactiveCircuitBreaker.Permitter first = connect(mount);
        assertTrue(first.tryAcquirePermit().isPresent());
        assertTrue(first.tryAcquirePermit().isPresent());
        ReactiveCircuitBreaker.Permitter second = connect(mount);
        assertTrue(second.tryAcquirePermit().isEmpty());

        first.disconnect();

        assertEquals(List.of(false, false), states);
        assertEquals(0, breaker.getMetrics().getNumberOfBufferedCalls());
        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isEmpty());
    }

    @Test
    public void disconnect_givenSeparateAdapters_expectsSharedRetryNotificationAndRestoredCapacity() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        List<Boolean> firstStates = new ArrayList<>();
        List<Boolean> secondStates = new ArrayList<>();
        ReactiveCircuitBreaker.Permitter first = connect(mount(breaker, state -> firstStates.add(state.tripped())));
        ReactiveCircuitBreaker.Permitter second = connect(mount(breaker, state -> secondStates.add(state.tripped())));
        assertTrue(first.tryAcquirePermit().isPresent());
        assertTrue(first.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isEmpty());

        first.disconnect();

        assertEquals(List.of(false, false), firstStates);
        assertEquals(List.of(false, false), secondStates);
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
        assertEquals(0, breaker.getMetrics().getNumberOfBufferedCalls());
        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isEmpty());
    }

    @Test
    public void disconnect_givenOtherConnectionWithOutstandingPermit_expectsOnlyOwnPermitReleased() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        ReactiveCircuitBreaker.Mount mount = mount(breaker, __ -> {});
        ReactiveCircuitBreaker.Permitter first = connect(mount);
        ReactiveCircuitBreaker.Permitter second = connect(mount);
        assertTrue(first.tryAcquirePermit().isPresent());
        ReactiveCircuitBreaker.Permit other = second.tryAcquirePermit().orElseThrow();

        first.disconnect();
        first.disconnect();

        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isEmpty());
        other.success();
        assertEquals(1, breaker.getMetrics().getNumberOfSuccessfulCalls());
    }

    @Test
    public void disconnect_givenSettledAndUnsettledPermits_expectsOnlyUnsettledPermitReleased() {
        CircuitBreaker breaker = newHalfOpenBreaker();
        ReactiveCircuitBreaker.Mount mount = mount(breaker, __ -> {});
        ReactiveCircuitBreaker.Permitter first = connect(mount);
        first.tryAcquirePermit().orElseThrow().success();
        ReactiveCircuitBreaker.Permit unsettled = first.tryAcquirePermit().orElseThrow();

        first.disconnect();
        unsettled.success();
        unsettled.error(new RuntimeException("after release"));
        ReactiveCircuitBreaker.Permitter second = connect(mount);

        assertEquals(1, breaker.getMetrics().getNumberOfSuccessfulCalls());
        assertEquals(0, breaker.getMetrics().getNumberOfFailedCalls());
        assertTrue(second.tryAcquirePermit().isPresent());
        assertTrue(second.tryAcquirePermit().isEmpty());
    }

    @Test
    public void success_givenConcurrentConflictingCallbacks_expectsExactlyOneOutcome() throws Exception {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permit permit =
                connect(mount(breaker, __ -> {})).tryAcquirePermit().orElseThrow();

        concurrently(permit::success, () -> permit.error(new RuntimeException("concurrent failure")));

        assertEquals(1, breaker.getMetrics().getNumberOfBufferedCalls());
        assertEquals(
                1,
                breaker.getMetrics().getNumberOfSuccessfulCalls()
                        + breaker.getMetrics().getNumberOfFailedCalls());
    }

    @Test
    public void disconnect_givenConcurrentSettlement_expectsAtMostOneOutcomeAndLateCallbacksIgnored() throws Exception {
        CircuitBreaker breaker = newBreaker();
        ReactiveCircuitBreaker.Permitter permitter = connect(mount(breaker, __ -> {}));
        ReactiveCircuitBreaker.Permit permit = permitter.tryAcquirePermit().orElseThrow();

        concurrently(
                permitter::disconnect, permit::success, () -> permit.error(new RuntimeException("racing failure")));
        int recorded = breaker.getMetrics().getNumberOfBufferedCalls();
        permit.success();
        permit.error(new RuntimeException("after disconnect"));
        permitter.disconnect();

        assertTrue(recorded <= 1);
        assertEquals(recorded, breaker.getMetrics().getNumberOfBufferedCalls());
    }

    private ReactiveCircuitBreaker.Mount mount(
            CircuitBreaker breaker, Consumer<? super ReactiveCircuitBreaker.State> consumer) {
        ReactiveCircuitBreaker.Mount mount = R4jCircuitBreaker.create(breaker).mount(consumer);
        mounts.add(mount);
        return mount;
    }

    private ReactiveCircuitBreaker.Permitter connect(ReactiveCircuitBreaker.Mount mount) {
        ReactiveCircuitBreaker.Permitter permitter = mount.connect();
        connections.add(permitter);
        return permitter;
    }

    private CircuitBreaker newBreaker() {
        return newBreaker(config().build());
    }

    private CircuitBreaker newBreaker(CircuitBreakerConfig config) {
        return CircuitBreaker.of("test", config);
    }

    private CircuitBreaker newHalfOpenBreaker() {
        CircuitBreaker breaker = newBreaker();
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
        return breaker;
    }

    private static CircuitBreakerConfig.Builder config() {
        return CircuitBreakerConfig.custom()
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .waitDurationInOpenState(Duration.ofHours(1))
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .permittedNumberOfCallsInHalfOpenState(2)
                .ignoreExceptions(IllegalArgumentException.class);
    }

    private static void concurrently(Runnable... actions) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(actions.length);
        var executor = Executors.newFixedThreadPool(actions.length);
        try {
            var tasks = new ArrayList<Future<?>>();
            for (Runnable action : actions) {
                tasks.add(executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    action.run();
                    return null;
                }));
            }
            for (var task : tasks) {
                task.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
