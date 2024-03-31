package io.atleon.core;

import io.atleon.util.Consuming;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.util.context.Context;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

final class AloErrorDelegatingOperator<T> extends FluxOperator<Alo<T>, Alo<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AloErrorDelegatingOperator.class);

    private final BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegator;

    AloErrorDelegatingOperator(
        Flux<Alo<T>> source,
        BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegator
    ) {
        super(source);
        this.delegator = delegator;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Alo<T>> actual) {
        source.subscribe(new AloErrorDelegatingSubscriber(actual));
    }

    private final class AloErrorDelegatingSubscriber implements CoreSubscriber<Alo<T>> {

        private final CoreSubscriber<? super Alo<T>> actual;

        private final SerialQueue<Consumer<Collection<Disposable>>> inFlight =
            SerialQueue.on(Collections.newSetFromMap(new IdentityHashMap<>()));

        private volatile boolean unsuccessfullyDone = false;

        private AloErrorDelegatingSubscriber(CoreSubscriber<? super Alo<T>> actual) {
            this.actual = actual;
        }

        @Override
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            Subscription decoratedSubscription = new Subscription() {
                @Override
                public void request(long n) {
                    s.request(n);
                }

                @Override
                public void cancel() {
                    unsuccessfullyDone = true;
                    try {
                        s.cancel();
                    } finally {
                        safelyDisposeAllInFlight();
                    }
                }
            };
            actual.onSubscribe(decoratedSubscription);
        }

        @Override
        public void onNext(Alo<T> alo) {
            Consumer<Throwable> nacknowledger = error -> alo.runInContext(() -> delegateAloError(alo, error));
            actual.onNext(alo.<T>propagator().create(alo.get(), alo.getAcknowledger(), nacknowledger));
        }

        @Override
        public void onError(Throwable t) {
            unsuccessfullyDone = true;
            safelyDisposeAllInFlight();
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        private void delegateAloError(Alo<T> alo, Throwable error) {
            AtomicReference<Disposable> disposableReference = new AtomicReference<>();
            ConnectableFlux<?> connectableFlux = delegateError(alo.get(), error)
                .doAfterTerminate(() -> inFlight.addAndDrain(disposables -> disposables.remove(disposableReference.get())))
                .publish();
            connectableFlux.subscribe(Consuming.noOp(), alo.getNacknowledger(), alo.getAcknowledger());
            inFlight.addAndDrain(disposables -> {
                if (!unsuccessfullyDone) {
                    connectableFlux.connect(disposable -> {
                        disposableReference.set(disposable);
                        disposables.add(disposable);
                    });
                }
            });
        }

        private void safelyDisposeAllInFlight() {
            try {
                inFlight.addAndDrain(disposables -> disposables.forEach(Disposable::dispose));
            } catch (Exception e) {
                LOGGER.error("Failed to dispose all in-flight error delegations. This may cause a memory leak...", e);
            }
        }

        private Flux<?> delegateError(T t, Throwable error) {
            try {
                return Flux.from(delegator.apply(t, error))
                    .onErrorMap(delegateError -> consolidateErrors(error, delegateError));
            } catch (Throwable delegateError) {
                return Flux.error(consolidateErrors(error, delegateError));
            }
        }

        private Throwable consolidateErrors(Throwable originalError, Throwable delegateError) {
            if (originalError != delegateError) {
                originalError.addSuppressed(delegateError);
            }
            return originalError;
        }
    }
}
