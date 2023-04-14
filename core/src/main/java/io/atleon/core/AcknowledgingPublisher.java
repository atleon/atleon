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
import java.util.function.Consumer;

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
            Subscriber<T> acknowledgingSubscriber = new AcknowledgingSubscriber<>(
                aloSource.getAcknowledger(),
                aloSource.getNacknowledger(),
                aloSource.propagator(),
                subscriber
            );
            aloSource.runInContext(() -> aloSource.get().subscribe(acknowledgingSubscriber));
        } else {
            throw new IllegalStateException("AcknowledgingPublisher may only be subscribed to once");
        }
    }

    private static final class AcknowledgingSubscriber<T> implements Subscriber<T> {

        private enum State {ACTIVE, IN_FLIGHT, EXECUTED}

        private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

        private final Collection<Reference<T>> unacknowledged = Collections.newSetFromMap(new IdentityHashMap<>());

        private final Runnable acknowledger;

        private final Consumer<? super Throwable> nacknowledger;

        private final AloFactory<T> factory;

        private final Subscriber<? super Alo<T>> subscriber;

        public AcknowledgingSubscriber(
            Runnable acknowledger,
            Consumer<? super Throwable> nacknowledger,
            AloFactory<T> factory,
            Subscriber<? super Alo<T>> subscriber
        ) {
            this.acknowledger = acknowledger;
            this.nacknowledger = nacknowledger;
            this.factory = factory;
            this.subscriber = subscriber;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            subscriber.onSubscribe(new ComposedSubscription(subscription::request, decorateCancellation(subscription)));
        }

        @Override
        public void onNext(T value) {
            // Use WeakReferences to track which emitted values have/haven't been acknowledged,
            // allowing those values to be garbage collected if their downstream transformations
            // do not require a handle on the originating value (and are processed asynchronously)
            Reference<T> valueReference = new WeakReference<>(Objects.requireNonNull(value, "Empty value emitted - Adhering to ReactiveStreams rule 2.13"));
            synchronized (unacknowledged) {
                // Ignoring race condition with IN_FLIGHT here; Execution is predicated on
                // obtaining a synchronization lock on `unacknowledged` which we own at this point
                if (state.get() == State.ACTIVE) {
                    unacknowledged.add(valueReference);
                }
            }
            // NOTE: We MUST pass the raw value here and not just the Reference. This is because it
            //       is possible for the Reference's referent to be garbage collected between the
            //       above Reference creation and calling `get` on said Reference
            subscriber.onNext(wrap(value, valueReference));
        }

        @Override
        public void onError(Throwable error) {
            subscriber.onError(error);
        }

        @Override
        public void onComplete() {
            if (state.compareAndSet(State.ACTIVE, State.IN_FLIGHT)) {
                maybeExecuteAcknowledger();
            }
            subscriber.onComplete();
        }

        private Runnable decorateCancellation(Subscription subscription) {
            return () -> {
                subscription.cancel();
                if (state.compareAndSet(State.ACTIVE, State.IN_FLIGHT)) {
                    maybeExecuteAcknowledger();
                }
            };
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
                    acknowledger.run();
                }
            }
        }

        private void maybeExecuteNacknowledger(Throwable error) {
            synchronized (unacknowledged) {
                if (state.compareAndSet(State.ACTIVE, State.EXECUTED) || state.compareAndSet(State.IN_FLIGHT, State.EXECUTED)) {
                    unacknowledged.clear();
                    nacknowledger.accept(error);
                }
            }
        }
    }
}