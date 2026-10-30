package io.atleon.core;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class AloCircuitBreakingOperator<T> extends FluxOperator<Alo<T>, Alo<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AloCircuitBreakingOperator.class);

    private final Flux<? extends ReactiveCircuitBreaker.State> currentStates;

    private final Supplier<? extends ReactiveCircuitBreaker.Permitter> permitConnector;

    private final int maxPrefetch;

    AloCircuitBreakingOperator(
            Publisher<Alo<T>> source,
            Flux<? extends ReactiveCircuitBreaker.State> currentStates,
            Supplier<? extends ReactiveCircuitBreaker.Permitter> permitConnector,
            int maxPrefetch) {
        super(Flux.from(source));
        this.currentStates = currentStates;
        this.permitConnector = permitConnector;
        this.maxPrefetch = maxPrefetch;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Alo<T>> actual) {
        source.subscribe(new AloCircuitBreakingSubscriber<>(currentStates, permitConnector.get(), maxPrefetch, actual));
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private static final class AloCircuitBreakingSubscriber<T> implements CoreSubscriber<Alo<T>>, Subscription {

        private static final int REQUEST_UPSTREAM_COMPLETED = -1;

        private static final int REQUEST_PUBLISH_ERROR = -2;

        private static final int REQUEST_DOWNSTREAM_CANCELED = -3;

        private static final AtomicLongFieldUpdater<AloCircuitBreakingSubscriber> OUTSTANDING_DOWNSTREAM_REQUEST =
                AtomicLongFieldUpdater.newUpdater(AloCircuitBreakingSubscriber.class, "outstandingDownstreamRequest");

        private static final AtomicLongFieldUpdater<AloCircuitBreakingSubscriber> OUTSTANDING_UPSTREAM_REQUEST =
                AtomicLongFieldUpdater.newUpdater(AloCircuitBreakingSubscriber.class, "outstandingUpstreamRequest");

        private static final AtomicIntegerFieldUpdater<AloCircuitBreakingSubscriber> DRAINS_IN_PROGRESS =
                AtomicIntegerFieldUpdater.newUpdater(AloCircuitBreakingSubscriber.class, "drainsInProgress");

        private static final AtomicReferenceFieldUpdater<AloCircuitBreakingSubscriber, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(AloCircuitBreakingSubscriber.class, Throwable.class, "error");

        private final ReactiveCircuitBreaker.Permitter permitter;

        private final int maxPrefetch;

        private final CoreSubscriber<? super Alo<T>> actual;

        private final Disposable breakerStateSubscription;

        private final Queue<Alo<T>> queue = new ConcurrentLinkedQueue<>();

        private Subscription parent;

        private volatile long outstandingDownstreamRequest = 0;

        private volatile long outstandingUpstreamRequest = 0;

        private volatile int drainsInProgress;

        private volatile Throwable error;

        private AloCircuitBreakingSubscriber(
                Flux<? extends ReactiveCircuitBreaker.State> currentStates,
                ReactiveCircuitBreaker.Permitter permitter,
                int maxPrefetch,
                CoreSubscriber<? super Alo<T>> actual) {
            this.permitter = permitter;
            this.maxPrefetch = maxPrefetch;
            this.actual = actual;
            this.breakerStateSubscription = currentStates
                    .filter(it -> !it.tripped())
                    .subscribe(__ -> drain(), error -> LOGGER.error("Breaker state change error", error));
        }

        @Override
        public @NonNull Context currentContext() {
            return actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            parent = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(Alo<T> alo) {
            queue.add(alo);
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
            if (ERROR.compareAndSet(this, null, t) && enterTerminableState(REQUEST_PUBLISH_ERROR)) {
                drain();
            }
        }

        @Override
        public void onComplete() {
            if (enterTerminableState(REQUEST_UPSTREAM_COMPLETED)) {
                drain();
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                Operators.addCap(OUTSTANDING_DOWNSTREAM_REQUEST, this, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            if (enterTerminableState(REQUEST_DOWNSTREAM_CANCELED)) {
                drain();
            }
        }

        private void drain() {
            if (DRAINS_IN_PROGRESS.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;
            do {
                long emitted = 0;
                long nvOutstandingUpstreamRequest;
                while ((nvOutstandingUpstreamRequest = outstandingUpstreamRequest) >= REQUEST_UPSTREAM_COMPLETED) {
                    try {
                        Alo<T> alo = reservePermissionToEmit(queue.peek()).orElse(null);
                        if (alo == null) {
                            break;
                        }
                        queue.remove();
                        actual.onNext(alo);
                        emitted++;
                    } catch (Throwable publishingError) {
                        LOGGER.error("Emission failure (violates Reactive Streams §2.13)", publishingError);
                        runSafely(parent::cancel, "parent::cancel");
                        ERROR.compareAndSet(this, null, publishingError);
                        enterTerminableState(REQUEST_PUBLISH_ERROR);
                    }
                }

                if (nvOutstandingUpstreamRequest >= 0) {
                    long upstreamRequestCapacity = maxPrefetch - nvOutstandingUpstreamRequest + emitted;
                    long toRequest = Math.min(upstreamRequestCapacity, outstandingDownstreamRequest);
                    long changeInRequest = toRequest - emitted;
                    if (changeInRequest != 0) {
                        OUTSTANDING_UPSTREAM_REQUEST.updateAndGet(this, it -> it >= 0 ? it + changeInRequest : it);
                    }
                    if (toRequest > 0) {
                        if (outstandingDownstreamRequest != Long.MAX_VALUE) {
                            OUTSTANDING_DOWNSTREAM_REQUEST.addAndGet(this, -toRequest);
                        }
                        parent.request(toRequest);
                    }
                } else if (nvOutstandingUpstreamRequest == REQUEST_DOWNSTREAM_CANCELED) {
                    OUTSTANDING_UPSTREAM_REQUEST.set(this, Long.MIN_VALUE);
                    runSafely(parent::cancel, "parent::cancel");
                    disposeSafely();
                    return;
                } else if (nvOutstandingUpstreamRequest == REQUEST_PUBLISH_ERROR) {
                    OUTSTANDING_UPSTREAM_REQUEST.set(this, Long.MIN_VALUE);
                    runSafely(() -> actual.onError(error), "actual::onError"); // error must be non-null
                    disposeSafely();
                    return;
                } else if (nvOutstandingUpstreamRequest == REQUEST_UPSTREAM_COMPLETED && queue.isEmpty()) {
                    OUTSTANDING_UPSTREAM_REQUEST.set(this, Long.MIN_VALUE);
                    runSafely(actual::onComplete, "actual::onComplete");
                    disposeSafely();
                    return;
                }

                missed = DRAINS_IN_PROGRESS.addAndGet(this, -missed);
            } while (missed != 0);
        }

        private void disposeSafely() {
            runSafely(breakerStateSubscription::dispose, "breakerStateSubscription::dispose");
            runSafely(permitter::disconnect, "permitter::disconnect");
        }

        private Optional<Alo<T>> reservePermissionToEmit(@Nullable Alo<T> alo) {
            if (alo == null) {
                return Optional.empty();
            }

            return permitter.tryAcquirePermit().map(permit -> {
                Runnable acknowledger = alo.getAcknowledger();
                Runnable decoratedAcknowledger = () -> {
                    runSafely(permit::success, "permit::success");
                    acknowledger.run();
                };

                Consumer<? super Throwable> nacknowledger = alo.getNacknowledger();
                Consumer<Throwable> decoratedNacknowledger = error -> {
                    runSafely(() -> permit.error(error), "permit::error");
                    nacknowledger.accept(error);
                };

                return alo.<T>propagator().create(alo.get(), decoratedAcknowledger, decoratedNacknowledger);
            });
        }

        private boolean enterTerminableState(long targetState) {
            return OUTSTANDING_UPSTREAM_REQUEST.getAndUpdate(this, it -> Math.min(it, targetState)) > targetState;
        }
    }
}
