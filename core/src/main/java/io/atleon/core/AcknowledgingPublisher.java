package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Publisher of {@link Alo} produced from a published mapping of another Alo. Takes care of
 * acknowledgement propagated from the original source by only executing acknowledgement iff all
 * resultant items are acknowledged <i>after upstream termination of this Publisher</i> OR one of
 * the emitted items is nacknowledged.
 *
 * @param <T> The type of data item emitted in Alo items of this Publisher
 */
final class AcknowledgingPublisher<T> implements Publisher<Alo<T>> {

    private final Alo<Publisher<T>> aloSource;

    private final AtomicBoolean subscribedOnce = new AtomicBoolean(false);

    private AcknowledgingPublisher(Alo<Publisher<T>> aloSource) {
        this.aloSource = aloSource;
    }

    public static <T> Publisher<Alo<T>> fromAloPublisher(Alo<Publisher<T>> aloPublisher) {
        return new AcknowledgingPublisher<>(aloPublisher);
    }

    @Override
    public void subscribe(Subscriber<? super Alo<T>> subscriber) {
        if (subscribedOnce.compareAndSet(false, true)) {
            Subscriber<T> acknowledgingSubscriber = new AcknowledgingSubscriber<>(aloSource, subscriber);
            aloSource.runInContext(() -> aloSource.get().subscribe(acknowledgingSubscriber));
        } else {
            throw new IllegalStateException("AcknowledgingPublisher may only be subscribed to once");
        }
    }

    private static final class AcknowledgingSubscriber<T> implements Subscriber<T> {

        private enum State {ACTIVE, IN_FLIGHT, EXECUTED}

        private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

        private final Collection<Reference<T>> unacknowledged = Collections.newSetFromMap(new IdentityHashMap<>());

        private final Alo<Publisher<T>> aloSource;

        private final AloFactory<T> factory;

        private final Subscriber<? super Alo<T>> subscriber;

        public AcknowledgingSubscriber(Alo<Publisher<T>> aloSource, Subscriber<? super Alo<T>> subscriber) {
            this.aloSource = aloSource;
            this.factory = aloSource.propagator();
            this.subscriber = subscriber;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            // We do NOT decorate cancellation with management of state and/or acknowledgement
            // management. This is due to the following logic, applicable upon cancellation:
            // 1) No matter what, we MUST propagate cancellation to obey ReactiveStreams rule 1.8
            // 2) We SHOULD NOT call any downstream subscriber methods since downstream has
            //    cancelled (signals would likely just be ignored)
            // 3) We SHOULD NOT negatively acknowledge since we have no error with which to do so
            // 4) We MUST NOT positively acknowledge as that could violate "at least once"
            // 5) We SHOULD NOT change state from ACTIVE, as that might interfere with reception of
            //    possibly-racing upstream termination signals
            // Given the above, we have exhausted all possible side-effects as actions that we, at
            // best, should not do. Therefore, we propagate the subscription as-is.
            subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(T value) {
            // Use WeakReferences to track which emitted values have/haven't been acknowledged,
            // allowing those values to be garbage collected if their downstream transformations
            // do not require a handle on the originating value (and are processed asynchronously)
            Reference<T> valueReference = new WeakReference<>(
                Objects.requireNonNull(value, "Empty value emitted - Adhering to ReactiveStreams rule 2.13")
            );
            synchronized (unacknowledged) {
                // Ignoring race condition with IN_FLIGHT here; Execution is predicated on
                // obtaining a synchronization lock on `unacknowledged` which we own at this point
                if (state.get() == State.ACTIVE) {
                    unacknowledged.add(valueReference);
                }
            }
            // We MUST pass the raw value here and not just the Reference. This is because it is
            // possible for the Reference's referent to be garbage collected between the above
            // Reference creation and calling `get` on said Reference
            subscriber.onNext(wrap(value, valueReference));
        }

        @Override
        public void onError(Throwable error) {
            AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

            // Delegation to failure strategy may result in execution, so must synchronize
            synchronized (unacknowledged) {
                if (state.compareAndSet(State.ACTIVE, State.IN_FLIGHT) &&
                    AloFailureStrategy.choose(subscriber).process(aloSource, error, errorReference::set)) {
                    // If we get here, the error we have received is the terminating signal, and
                    // that error has been successfully handled by the chosen failure strategy.
                    // We therefore mark our state as executed, which we can safely do since other
                    // executions require a lock on unacknowledged. This will avoid any further
                    // unnecessary error propagation.
                    state.set(State.EXECUTED);
                } else {
                    // If we get here, the error was either not the terminating signal or handling
                    // the error has been delegated to us. Make sure the negative acknowledgement
                    // is executed if it's possible to do so
                    maybeExecuteNacknowledger(error);
                }
            }

            Throwable errorToEmit = errorReference.get();
            if (errorToEmit != null) {
                // Failure strategy must have requested that we emit the error
                subscriber.onError(errorToEmit);
            } else {
                // The error was either successfully processed or delegated via negative
                // acknowledgement. Do not unnecessarily emit an error that has been handled.
                subscriber.onComplete();
            }
        }

        @Override
        public void onComplete() {
            if (state.compareAndSet(State.ACTIVE, State.IN_FLIGHT)) {
                maybeExecuteAcknowledger();
            }
            subscriber.onComplete();
        }

        private Alo<T> wrap(T value, Reference<T> valueReference) {
            return factory.create(
                value,
                () -> {
                    synchronized (unacknowledged) {
                        if (unacknowledged.remove(valueReference)) {
                            maybeExecuteAcknowledger();
                        }
                    }
                },
                error -> {
                    synchronized (unacknowledged) {
                        if (unacknowledged.contains(valueReference)) {
                            maybeExecuteNacknowledger(error);
                        }
                    }
                }
            );
        }

        private void maybeExecuteAcknowledger() {
            synchronized (unacknowledged) {
                if (unacknowledged.isEmpty() && state.compareAndSet(State.IN_FLIGHT, State.EXECUTED)) {
                    Alo.acknowledge(aloSource);
                }
            }
        }

        private void maybeExecuteNacknowledger(Throwable error) {
            synchronized (unacknowledged) {
                if (state.getAndSet(State.EXECUTED) != State.EXECUTED) {
                    unacknowledged.clear();
                    Alo.nacknowledge(aloSource, error);
                }
            }
        }
    }
}