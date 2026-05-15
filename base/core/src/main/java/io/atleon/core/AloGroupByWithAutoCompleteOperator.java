package io.atleon.core;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Enables grouping of related {@link Alo} elements into serialized {@link AloGroupedFlux} streams.
 * In contrast to standard {@link Flux#groupBy(Function)}, this operator leverages the fact that
 * emitted elements must signal processing completion through acknowledgement, which enables
 * automatic resource cleanup (i.e. completion of processing groups) when all emitted elements in a
 * processing group have completed processing.
 * <p>
 * This operator makes the following assumptions about streams to which it may be applied:
 * <ul>
 *     <li>It is desired that upstream errors terminate in-flight processing before propagating
 *     such errors to the downstream group emission subscriber.</li>
 *     <li>Downstream cancellation of group emission is intended to signal both upstream
 *     cancellation and termination of emitted processing groups.</li>
 *     <li>Upon termination of processing due to either upstream error or downstream cancellation,
 *     the upstream source handles any necessary re-emission of emitted elements that were
 *     in-flight at time of termination.</li>
 *     <li>Downstream group processing operators do <i>not</i> impose absolute request limiting,
 *     i.e. downstream operators on emitted groups either request infinite elements, or never stop
 *     requesting elements while emission continues.</li>
 * </ul>
 * Given the above usage assumptions, this operator does <i>not</i> make thorough attempts to
 * positively or negatively acknowledge emitted {@link Alo} elements on either upstream
 * errors or downstream cancellation.
 *
 * @param <K> Type of keys extracted from upstream {@link Alo} elements used for process grouping
 * @param <T> Type of data referenced by upstream {@link Alo} elements
 */
final class AloGroupByWithAutoCompleteOperator<K, T> extends FluxOperator<Alo<T>, AloGroupedFlux<K, T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AloGroupByWithAutoCompleteOperator.class);

    private final int maxInFlight;

    private final Function<? super T, ? extends K> keyExtractor;

    AloGroupByWithAutoCompleteOperator(
            Flux<Alo<T>> source, int maxInFlight, Function<? super T, ? extends K> keyExtractor) {
        super(source);
        this.maxInFlight = maxInFlight;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void subscribe(CoreSubscriber<? super AloGroupedFlux<K, T>> actual) {
        source.subscribe(new AloGroupByWithAutoCompleteSubscriber<>(maxInFlight, keyExtractor, actual));
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private static final class AloGroupByWithAutoCompleteSubscriber<K, T>
            implements CoreSubscriber<Alo<T>>, ProcessingGroupListener<K, Alo<T>>, Subscription {

        private static final long IN_FLIGHT_CAPACITY_UPSTREAM_TERMINATED = -1;

        private static final long IN_FLIGHT_CAPACITY_PUBLISHING_ERROR = -2;

        private static final long IN_FLIGHT_CAPACITY_DOWNSTREAM_CANCELED = -3;

        private static final AtomicLongFieldUpdater<AloGroupByWithAutoCompleteSubscriber> FREE_IN_FLIGHT_CAPACITY =
                AtomicLongFieldUpdater.newUpdater(AloGroupByWithAutoCompleteSubscriber.class, "freeInFlightCapacity");

        private static final AtomicLongFieldUpdater<AloGroupByWithAutoCompleteSubscriber> REQUEST_OUTSTANDING =
                AtomicLongFieldUpdater.newUpdater(AloGroupByWithAutoCompleteSubscriber.class, "requestOutstanding");

        private static final AtomicIntegerFieldUpdater<AloGroupByWithAutoCompleteSubscriber> DRAINS_IN_PROGRESS =
                AtomicIntegerFieldUpdater.newUpdater(AloGroupByWithAutoCompleteSubscriber.class, "drainsInProgress");

        private static final AtomicReferenceFieldUpdater<AloGroupByWithAutoCompleteSubscriber, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(
                        AloGroupByWithAutoCompleteSubscriber.class, Throwable.class, "error");

        private final int maxInFlight;

        private final Function<? super T, ? extends K> keyExtractor;

        private final CoreSubscriber<? super AloGroupedFlux<K, T>> actual;

        private final Queue<Alo<T>> nextQueue = new ConcurrentLinkedQueue<>();

        private final Map<K, ProcessingGroup<K, Alo<T>>> processingGroups = new HashMap<>();

        private final Queue<ProcessingGroup<K, Alo<T>>> emittableGroups = new ArrayDeque<>();

        private final Queue<ProcessingGroup<K, Alo<T>>> emptiedGroups = new ConcurrentLinkedQueue<>();

        // Intentionally not volatile per Reactor convention
        private Subscription parent;

        // This counter doubles as both our publishing state (via polarity: non-negative == ACTIVE,
        // negative == TERMINABLE or TERMINATED) and (when non-negative) our count of available
        // upstream request capacity. As such, when this first becomes negative, it means we have
        // entered a TERMINABLE state (error or cancellation). When it is set to Long.MIN_VALUE it
        // means we have signaled (or are signaling) termination to either/both upstream source and
        // downstream subscriber.
        private volatile long freeInFlightCapacity;

        private volatile long requestOutstanding;

        private volatile int drainsInProgress;

        private volatile Throwable error;

        private AloGroupByWithAutoCompleteSubscriber(
                int maxInFlight,
                Function<? super T, ? extends K> keyExtractor,
                CoreSubscriber<? super AloGroupedFlux<K, T>> actual) {
            if (maxInFlight <= 0) {
                throw new IllegalArgumentException("maxInFlight must be > 0");
            }
            this.maxInFlight = maxInFlight;
            this.keyExtractor = keyExtractor;
            this.actual = actual;
        }

        @Override
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!Operators.validate(parent, s)) {
                return;
            }

            parent = s;
            FREE_IN_FLIGHT_CAPACITY.set(this, maxInFlight == Integer.MAX_VALUE ? Long.MAX_VALUE : maxInFlight);
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(Alo<T> alo) {
            nextQueue.add(alo);
            drain();
        }

        @Override
        public void onError(Throwable t) {
            // It is important that the error CAS comes before entering terminable state, since
            // it's possible that another thread is concurrently draining, and we don't want to
            // risk such a thread seeing a terminable state and erroneously emitting completion
            // when we're about to set an error. Note that this could race with downstream
            // cancellation, but it's not a spec violation if upstream termination is concurrent
            // with downstream cancel.
            if (ERROR.compareAndSet(this, null, t) && enterTerminableStateFromUpstreamSignal()) {
                drain();
            }
        }

        @Override
        public void onComplete() {
            if (enterTerminableStateFromUpstreamSignal()) {
                drain();
            }
        }

        @Override
        public void onFatalEmissionFailure(Throwable error) {
            handleFatalPublishingFailure(error);
        }

        @Override
        public void onElementCompletedProcessing(ProcessingGroup<K, Alo<T>> group, boolean groupEmptied) {
            long previousFreeInFlightCapacity = maxInFlight != Integer.MAX_VALUE
                    ? FREE_IN_FLIGHT_CAPACITY.getAndUpdate(this, it -> it >= 0 ? it + 1 : it)
                    : Long.MAX_VALUE;
            if (groupEmptied) {
                emptiedGroups.add(group);
                drain();
            } else if (previousFreeInFlightCapacity != Long.MAX_VALUE) {
                drain();
            }
        }

        @Override
        public void request(long n) {
            if (!Operators.validate(n)) {
                return;
            }

            Operators.addCap(REQUEST_OUTSTANDING, this, n);
            drain();
        }

        @Override
        public void cancel() {
            if (enterTerminableState(IN_FLIGHT_CAPACITY_DOWNSTREAM_CANCELED)) {
                drain();
            }
        }

        private void handleFatalPublishingFailure(Throwable error) {
            if (enterTerminableState(IN_FLIGHT_CAPACITY_PUBLISHING_ERROR)) {
                onError(error);
                drain();
            }
        }

        private void drain() {
            if (DRAINS_IN_PROGRESS.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;
            do {
                // Complete any groups that have been emptied (and still are). A group that WAS
                // emptied may not still be empty in the case that the drain loop was just
                // previously invoked with a group that was emptied after this check, but then
                // had more elements emitted into it (in the next code block).
                ProcessingGroup<K, Alo<T>> emptied;
                while ((emptied = emptiedGroups.poll()) != null) {
                    if (!emptied.isEmpty()) {
                        continue;
                    }
                    ProcessingGroup<K, Alo<T>> removed = processingGroups.remove(emptied.key());
                    if (removed != null) {
                        runSafely(removed::complete, "group::complete");
                    }
                }

                // Emit next Alos into corresponding groups, enqueuing new groups for emission
                Alo<T> next;
                while (mayContinueEmission() && (next = nextQueue.poll()) != null) {
                    try {
                        T t = next.get();
                        AloFactory<T> propagator = next.propagator();
                        processingGroups
                                .computeIfAbsent(keyExtractor.apply(t), this::enqueueEmittableGroup)
                                .next(next.getAcknowledger(), next.getNacknowledger(), it -> newAlo(propagator, t, it));
                    } catch (Throwable publishingError) {
                        handleFatalPublishingFailure(publishingError);
                    }
                }

                // Emit as many queued groups as possible and update outstanding request
                long maxToEmit = requestOutstanding;
                long emitted = 0L;
                ProcessingGroup<K, Alo<T>> emittable;
                while (emitted < maxToEmit && mayContinueEmission() && (emittable = emittableGroups.poll()) != null) {
                    try {
                        actual.onNext(new AloGroupedFlux<>(emittable.elements(), emittable.key()));
                        emitted++;
                    } catch (Throwable publishingError) {
                        handleFatalPublishingFailure(publishingError);
                    }
                }
                Operators.produced(REQUEST_OUTSTANDING, this, emitted);

                // Propagate subscription signals to parent and/or emit termination signals if now
                // is the time to do so.
                long nvFreeInFlightCapacity = freeInFlightCapacity;
                if (nvFreeInFlightCapacity > 0) {
                    parent.request(nvFreeInFlightCapacity);
                    FREE_IN_FLIGHT_CAPACITY.updateAndGet(this, it -> it >= 0 ? it - nvFreeInFlightCapacity : it);
                } else if (nvFreeInFlightCapacity < 0) {
                    if (nvFreeInFlightCapacity == IN_FLIGHT_CAPACITY_DOWNSTREAM_CANCELED) {
                        FREE_IN_FLIGHT_CAPACITY.set(this, Long.MIN_VALUE);
                        runSafely(parent::cancel, "parent::cancel");
                        terminateProcessingGroups(ProcessingGroup::complete, "complete");
                        return;
                    } else if (nvFreeInFlightCapacity == IN_FLIGHT_CAPACITY_PUBLISHING_ERROR) {
                        runSafely(parent::cancel, "parent::cancel");
                    }

                    Throwable nvError = error;
                    if (nvError != null) {
                        FREE_IN_FLIGHT_CAPACITY.set(this, Long.MIN_VALUE);
                        terminateProcessingGroups(ProcessingGroup::complete, "error");
                        terminateActual(it -> it.onError(nvError), "error");
                        return;
                    } else if (processingGroups.isEmpty()) {
                        FREE_IN_FLIGHT_CAPACITY.set(this, Long.MIN_VALUE);
                        terminateActual(Subscriber::onComplete, "complete");
                        return;
                    }
                }

                missed = DRAINS_IN_PROGRESS.addAndGet(this, -missed);
            } while (missed != 0);
        }

        private ProcessingGroup<K, Alo<T>> enqueueEmittableGroup(K key) {
            ProcessingGroup<K, Alo<T>> emittable = new ProcessingGroup<>(key, this);
            emittableGroups.add(emittable);
            return emittable;
        }

        private void terminateProcessingGroups(Consumer<ProcessingGroup<?, ?>> groupConsumer, String method) {
            processingGroups.values().forEach(it -> runSafely(() -> groupConsumer.accept(it), "group::" + method));
        }

        private void terminateActual(Consumer<Subscriber<?>> actualConsumer, String method) {
            runSafely(() -> actualConsumer.accept(actual), "actual::" + method);
        }

        private boolean mayContinueEmission() {
            return freeInFlightCapacity >= IN_FLIGHT_CAPACITY_UPSTREAM_TERMINATED;
        }

        private boolean enterTerminableStateFromUpstreamSignal() {
            return enterTerminableState(IN_FLIGHT_CAPACITY_UPSTREAM_TERMINATED);
        }

        private boolean enterTerminableState(long targetState) {
            return FREE_IN_FLIGHT_CAPACITY.getAndUpdate(this, it -> Math.min(it, targetState)) > targetState;
        }

        private static <T> Alo<T> newAlo(AloFactory<T> aloFactory, T t, Acknowledgement acknowledgement) {
            return aloFactory.create(t, acknowledgement::positive, acknowledgement::negative);
        }
    }

    private static final class ProcessingGroup<K, T> {

        private static final AtomicLongFieldUpdater<ProcessingGroup> IN_FLIGHT =
                AtomicLongFieldUpdater.newUpdater(ProcessingGroup.class, "inFlight");

        private final K key;

        private final ProcessingGroupListener<K, T> listener;

        private final Sinks.Many<T> sink = Sinks.unsafe().many().unicast().onBackpressureBuffer();

        private volatile long inFlight;

        public ProcessingGroup(K key, ProcessingGroupListener<K, T> listener) {
            this.key = key;
            this.listener = listener;
        }

        public void next(
                Runnable acknowledger,
                Consumer<? super Throwable> nacknowledger,
                Function<Acknowledgement, T> elementFactory) {
            IN_FLIGHT.incrementAndGet(this);

            Acknowledgement acknowledgement = Acknowledgement.create(
                    () -> {
                        acknowledger.run();
                        elementCompletedProcessing();
                    },
                    error -> {
                        nacknowledger.accept(error);
                        elementCompletedProcessing();
                    });

            sink.emitNext(elementFactory.apply(acknowledgement), this::handleEmissionFailure);
        }

        public void complete() {
            sink.emitComplete(this::handleEmissionFailure);
        }

        public K key() {
            return key;
        }

        public Flux<T> elements() {
            return sink.asFlux();
        }

        public boolean isEmpty() {
            return inFlight == 0;
        }

        private boolean handleEmissionFailure(SignalType signal, Sinks.EmitResult result) {
            String message = String.format("Emission failure: key=%s signal=%s result=%s", key, signal, result);
            if (signal != SignalType.ON_NEXT && result == Sinks.EmitResult.FAIL_CANCELLED) {
                LOGGER.debug(message);
            } else {
                listener.onFatalEmissionFailure(new IllegalStateException(message));
            }
            return false;
        }

        private void elementCompletedProcessing() {
            listener.onElementCompletedProcessing(this, IN_FLIGHT.decrementAndGet(this) == 0L);
        }
    }

    private interface ProcessingGroupListener<K, T> {

        void onFatalEmissionFailure(Throwable error);

        void onElementCompletedProcessing(ProcessingGroup<K, T> group, boolean groupEmptied);
    }
}
