package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

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

        private enum State {ACTIVE, IN_FLIGHT, EXECUTING, EXECUTED}

        private static final AtomicReferenceFieldUpdater<AcknowledgingSubscriber, State> STATE =
            AtomicReferenceFieldUpdater.newUpdater(AcknowledgingSubscriber.class, State.class, "state");

        private static final AtomicLongFieldUpdater<AcknowledgingSubscriber> COUNT =
            AtomicLongFieldUpdater.newUpdater(AcknowledgingSubscriber.class, "count");

        private final Alo<Publisher<T>> aloSource;

        private final AloFactory<T> factory;

        private final Subscriber<? super Alo<T>> subscriber;

        private volatile State state = State.ACTIVE;

        private volatile long count = 1L; // Initialize to one for onComplete

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
            //    cancelled (signals likely ignored and follows ReactiveStreams rule 1.8+TCK)
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
            Acknowledgement acknowledgement = Acknowledgement.create(this::inFlightAcknowledged, this::inFlightNacknowledged);

            COUNT.incrementAndGet(this);
            subscriber.onNext(factory.create(value, acknowledgement::positive, acknowledgement::negative));
        }

        @Override
        public void onError(Throwable error) {
            AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

            if (STATE.compareAndSet(this, State.ACTIVE, State.EXECUTING) &&
                AloFailureStrategy.choose(subscriber).process(aloSource, error, errorReference::set)) {
                // If we get here, the error we have received is the terminating signal, and that
                // error has been successfully handled by the chosen failure strategy. We therefore
                // clean up by marking our state as EXECUTED, which we can safely do since we know
                // the state MUST currently be EXECUTING, and no further termination action(s) can
                // (or should) be executed.
                STATE.set(this, State.EXECUTED);
            } else {
                // If we get here, the error was either not the terminating signal (and therefore
                // negative acknowledgement has been executed, or we raced with it), or handling
                // the error has been delegated to us. Make sure negative acknowledgement is
                // executed if it's possible to do so
                maybeExecuteNacknowledger(error, priorState -> priorState == State.ACTIVE || priorState == State.EXECUTING);
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
            if (STATE.compareAndSet(this, State.ACTIVE, State.IN_FLIGHT) && COUNT.decrementAndGet(this) == 0L) {
                maybeExecuteAcknowledger();
            }
            subscriber.onComplete();
        }

        private void inFlightAcknowledged() {
            if (COUNT.decrementAndGet(this) == 0L) { // Can only reach zero after onComplete
                maybeExecuteAcknowledger();
            }
        }

        private void inFlightNacknowledged(Throwable error) {
            maybeExecuteNacknowledger(error, priorState -> priorState == State.ACTIVE || priorState == State.IN_FLIGHT);
        }

        private void maybeExecuteAcknowledger() {
            if (STATE.compareAndSet(this, State.IN_FLIGHT, State.EXECUTED)) {
                Alo.acknowledge(aloSource);
            }
        }

        private void maybeExecuteNacknowledger(Throwable error, Predicate<State> priorStateMustMatch) {
            if (priorStateMustMatch.test(STATE.getAndSet(this, State.EXECUTED))) {
                Alo.nacknowledge(aloSource, error);
            }
        }
    }
}