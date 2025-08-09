package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

final class SendPublisher<K, V, T> implements Publisher<KafkaSenderResult<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendPublisher.class);

    private final Publisher<? extends KafkaSenderRecord<K, V, T>> source;

    private final Function<Subscriber<? super KafkaSenderResult<T>>, ? extends Send<K, V, T>> sender;

    private SendPublisher(
        Publisher<? extends KafkaSenderRecord<K, V, T>> source,
        Function<Subscriber<? super KafkaSenderResult<T>>, ? extends Send<K, V, T>> sender
    ) {
        this.source = source;
        this.sender = sender;
    }

    public static <K, V, T> SendPublisher<K, V, T> immediateError(
        KafkaSenderOptions<K, V> options,
        SendingProducer<K, V> producer,
        Publisher<KafkaSenderRecord<K, V, T>> senderRecords
    ) {
        return new SendPublisher<>(senderRecords, it -> new CompletingSend<>(options, producer, it, false));
    }

    public static <K, V, T> SendPublisher<K, V, T> delegateError(
        KafkaSenderOptions<K, V> options,
        SendingProducer<K, V> producer,
        Publisher<KafkaSenderRecord<K, V, T>> senderRecords
    ) {
        return new SendPublisher<>(senderRecords, it -> new CompletingSend<>(options, producer, it, true));
    }

    public static <K, V, T> SendPublisher<K, V, T> delayError(
        KafkaSenderOptions<K, V> options,
        SendingProducer<K, V> producer,
        Publisher<KafkaSenderRecord<K, V, T>> senderRecords
    ) {
        return new SendPublisher<>(senderRecords, it -> new ErrorDelayingSend<>(options, producer, it));
    }

    @Override
    public void subscribe(Subscriber<? super KafkaSenderResult<T>> actual) {
        source.subscribe(sender.apply(actual));
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private static abstract class Send<K, V, T> implements CoreSubscriber<KafkaSenderRecord<K, V, T>>, Subscription {

        private enum State {ACTIVE, TERMINABLE, TERMINATED}

        private static final AtomicLongFieldUpdater<Send> IN_FLIGHT =
            AtomicLongFieldUpdater.newUpdater(Send.class, "inFlight");

        private static final AtomicLongFieldUpdater<Send> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(Send.class, "requested");

        private static final AtomicReferenceFieldUpdater<Send, State> SUBSCRIPTION_STATE =
            AtomicReferenceFieldUpdater.newUpdater(Send.class, State.class, "subscriptionState");

        private static final AtomicIntegerFieldUpdater<Send> SUBSCRIPTION_DRAINS_IN_PROGRESS =
            AtomicIntegerFieldUpdater.newUpdater(Send.class, "subscriptionDrainsInProgress");

        private final KafkaSenderOptions<K, V> options;

        private final SendingProducer<K, V> producer;

        private final Context subscriberContext;

        private final SerialQueue<Consumer<Subscriber<? super KafkaSenderResult<T>>>> emissionQueue;

        private Subscription parent;

        // This counter doubles as both our publishing state (via polarity: positive == ACTIVE,
        // negative == TERMINABLE, zero == TERMINATED) and our count of in-flight sent records (via
        // magnitude: subtract 1 if positive, negate if negative). The extra/initializing count of
        // 1 is in reserve for termination of this subscriber (self). As such, when this becomes
        // non-positive, it means a terminating signal has been received from upstream OR a fatal
        // error has been encountered on sending. When it becomes zero, it means termination has
        // been enqueued to the downstream subscriber.
        private volatile long inFlight = 1;

        private volatile long requested = 0;

        private volatile State subscriptionState = State.ACTIVE;

        private volatile int subscriptionDrainsInProgress = 0;

        protected Send(
            KafkaSenderOptions<K, V> options,
            SendingProducer<K, V> producer,
            CoreSubscriber<? super KafkaSenderResult<T>> actual
        ) {
            this.options = options;
            this.producer = producer;
            this.subscriberContext = actual.currentContext();
            this.emissionQueue = SerialQueue.on(actual);
        }

        @Override
        public Context currentContext() {
            return subscriberContext;
        }

        @Override
        public void onSubscribe(Subscription s) {
            parent = s;
            emissionQueue.addAndDrain(subscriber -> subscriber.onSubscribe(this));
        }

        @Override
        public void onNext(KafkaSenderRecord<K, V, T> senderRecord) {
            if (IN_FLIGHT.getAndUpdate(this, it -> it > 0 ? it + 1 : it) <= 0) {
                return;
            }

            T correlationMetadata = senderRecord.correlationMetadata();
            producer.sendSafely(senderRecord, () -> subscriptionState == State.ACTIVE, (recordMetadata, exception) -> {
                if (exception == null) {
                    enqueueNext(KafkaSenderResult.success(recordMetadata, correlationMetadata));
                } else if (shouldEmitFailureAsResult(exception) && !KafkaErrors.isFatalSendFailure(exception)) {
                    enqueueNext(KafkaSenderResult.failure(exception, correlationMetadata));
                } else if (subscriptionState == State.ACTIVE) {
                    cancel();
                    onError(exception);
                }
            });
        }

        @Override
        public void onError(Throwable t) {
            if (IN_FLIGHT.getAndSet(this, 0) != 0) {
                enqueueTermination(subscriber -> subscriber.onError(t));
            }
        }

        @Override
        public void onComplete() {
            if (IN_FLIGHT.getAndUpdate(this, it -> it > 0 ? 1 - it : it) == 1) {
                enqueueTermination(completionTerminator());
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                Operators.addCap(REQUESTED, this, n);
                drainSubscription();
            }
        }

        @Override
        public void cancel() {
            if (SUBSCRIPTION_STATE.compareAndSet(this, State.ACTIVE, State.TERMINABLE)) {
                drainSubscription();
            }
        }

        protected abstract boolean shouldEmitFailureAsResult(Exception failure);

        protected abstract Consumer<Subscriber<?>> completionTerminator();

        private void enqueueNext(KafkaSenderResult<T> result) {
            emissionQueue.addAndDrain(subscriber -> {
                if (inFlight == 0 || subscriptionState != State.ACTIVE) {
                    return;
                }

                try {
                    subscriber.onNext(result);
                } catch (Throwable error) {
                    LOGGER.error("Emission failure (§2.13)", error);
                    cancel();
                    onError(error);
                    return;
                }

                long previousInFlight = IN_FLIGHT.getAndUpdate(this, count -> {
                    if (count > 0) {
                        return count - 1;
                    } else if (count == 0) {
                        return count;
                    } else {
                        return count + 1;
                    }
                });

                // If the previous in-flight count was positive, then we are not yet eligible for
                // termination, and we should ensure that any capacity freed by emitting the last
                // result is reflected by upstream demand. Only if the previous count was exactly
                // -1 do we know that we are eligible for termination, and that this was the last
                // in-flight result to emit, so we can emit termination. Note that because onError
                // and onComplete immediately make the in-flight count non-positive, it will never
                // be those calling threads that could also invoke drainSubscription (which would
                // be a violation of §2.3).
                if (previousInFlight > 0) {
                    drainSubscription();
                } else if (previousInFlight == -1 && subscriptionState == State.ACTIVE) {
                    enqueueTermination(completionTerminator());
                }
            });
        }

        private void enqueueTermination(Consumer<Subscriber<?>> terminator) {
            // This is only ever invoked at-most-once after termination has been signalled from
            // upstream AND inFlight has reached zero. This could race with cancellation, but it's
            // not a spec violation if upstream termination is concurrent with downstream cancel.
            emissionQueue.addAndDrain(subscriber -> runSafely(() -> terminator.accept(subscriber), "Termination"));
        }

        private void drainSubscription() {
            if (subscriptionState == State.TERMINATED || SUBSCRIPTION_DRAINS_IN_PROGRESS.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;
            do {
                if (subscriptionState == State.ACTIVE) {
                    long toRequest = Math.min(freeInFlightSendCapacity(), requested);

                    if (toRequest > 0L) {
                        if (requested != Long.MAX_VALUE) {
                            REQUESTED.addAndGet(this, -toRequest);
                        }
                        runSafely(() -> parent.request(toRequest), "parent::request");
                    }
                } else if (SUBSCRIPTION_STATE.compareAndSet(this, State.TERMINABLE, State.TERMINATED)) {
                    runSafely(parent::cancel, "parent::cancel");
                }

                missed = SUBSCRIPTION_DRAINS_IN_PROGRESS.addAndGet(this, -missed);
            } while (missed != 0);
        }

        private long freeInFlightSendCapacity() {
            if (options.maxInFlight() == Integer.MAX_VALUE) {
                return Long.MAX_VALUE;
            } else {
                long nvInFlight = inFlight;
                // When the in-flight count is positive, it means there is still an extra count of
                // 1 to represent the current publishing state of ACTIVE, so we need to add that
                // back after subtracting from the max in-flight. When non-positive, the extra
                // count has already been removed and the total count has been negated, so we just
                // need to add.
                return nvInFlight > 0 ? (options.maxInFlight() - nvInFlight + 1) : (options.maxInFlight() + nvInFlight);
            }
        }
    }

    private static final class CompletingSend<K, V, T> extends Send<K, V, T> {

        private final boolean emitFailuresAsResults;

        public CompletingSend(
            KafkaSenderOptions<K, V> options,
            SendingProducer<K, V> producer,
            Subscriber<? super KafkaSenderResult<T>> actual,
            boolean emitFailuresAsResults
        ) {
            super(options, producer, Operators.toCoreSubscriber(actual));
            this.emitFailuresAsResults = emitFailuresAsResults;
        }

        @Override
        protected boolean shouldEmitFailureAsResult(Exception failure) {
            return emitFailuresAsResults;
        }

        @Override
        protected Consumer<Subscriber<?>> completionTerminator() {
            return Subscriber::onComplete;
        }
    }

    private static final class ErrorDelayingSend<K, V, T> extends Send<K, V, T> {

        private static final AtomicReferenceFieldUpdater<ErrorDelayingSend, Exception> FIRST_FAILURE =
            AtomicReferenceFieldUpdater.newUpdater(ErrorDelayingSend.class, Exception.class, "firstFailure");

        private volatile Exception firstFailure;

        public ErrorDelayingSend(
            KafkaSenderOptions<K, V> options,
            SendingProducer<K, V> producer,
            Subscriber<? super KafkaSenderResult<T>> actual
        ) {
            super(options, producer, Operators.toCoreSubscriber(actual));
        }

        @Override
        protected boolean shouldEmitFailureAsResult(Exception failure) {
            FIRST_FAILURE.compareAndSet(this, null, failure);
            return true;
        }

        @Override
        protected Consumer<Subscriber<?>> completionTerminator() {
            Exception nvFirstFailure = firstFailure;
            return nvFirstFailure != null ? subscriber -> subscriber.onError(nvFirstFailure) : Subscriber::onComplete;
        }
    }
}
