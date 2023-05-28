package io.atleon.core;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class AloQueueingOperator<T, V> implements Publisher<Alo<V>> {

    private final Publisher<? extends T> source;

    private final Function<T, ?> groupExtractor;

    private final Supplier<? extends AcknowledgementQueue> queueSupplier;

    private final AloQueueListener listener;

    private final AloComponentExtractor<T, V> componentExtractor;

    private final AloFactory<V> factory;

    private final long maxInFlight;

    AloQueueingOperator(
        Publisher<? extends T> source,
        Function<T, ?> groupExtractor,
        Supplier<? extends AcknowledgementQueue> queueSupplier,
        AloQueueListener listener,
        AloComponentExtractor<T, V> componentExtractor,
        AloFactory<V> factory,
        long maxInFlight
    ) {
        this.source = source;
        this.groupExtractor = groupExtractor;
        this.queueSupplier = queueSupplier;
        this.listener = listener;
        this.componentExtractor = componentExtractor;
        this.factory = factory;
        this.maxInFlight = maxInFlight;
    }

    @Override
    public void subscribe(Subscriber<? super Alo<V>> actual) {
        Subscriber<T> queueingSubscriber = new AloQueueingSubscriber<>(
            actual,
            groupExtractor,
            queueSupplier,
            listener,
            componentExtractor,
            factory,
            maxInFlight
        );
        source.subscribe(queueingSubscriber);
    }

    private static final class AloQueueingSubscriber<T, V> implements Subscriber<T>, Subscription {

        private static final AtomicLongFieldUpdater<AloQueueingSubscriber> FREE_CAPACITY =
            AtomicLongFieldUpdater.newUpdater(AloQueueingSubscriber.class, "freeCapacity");

        private static final AtomicLongFieldUpdater<AloQueueingSubscriber> REQUEST_OUTSTANDING =
            AtomicLongFieldUpdater.newUpdater(AloQueueingSubscriber.class, "requestOutstanding");

        private static final AtomicIntegerFieldUpdater<AloQueueingSubscriber> REQUESTS_IN_PROGRESS =
            AtomicIntegerFieldUpdater.newUpdater(AloQueueingSubscriber.class, "requestsInProgress");

        private final Subscriber<? super Alo<V>> actual;

        private final Function<T, ?> groupExtractor;

        private final Supplier<? extends AcknowledgementQueue> queueSupplier;

        private final AloQueueListener listener;

        private final AloComponentExtractor<T, V> componentExtractor;

        private final AloFactory<V> factory;

        private final Map<Object, AcknowledgementQueue> queuesByGroup = new ConcurrentHashMap<>();

        private Subscription parent;

        private volatile long freeCapacity;

        private volatile long requestOutstanding;

        private volatile int requestsInProgress;

        public AloQueueingSubscriber(
            Subscriber<? super Alo<V>> actual,
            Function<T, ?> groupExtractor,
            Supplier<? extends AcknowledgementQueue> queueSupplier,
            AloQueueListener listener,
            AloComponentExtractor<T, V> componentExtractor,
            AloFactory<V> factory,
            long maxInFlight
        ) {
            this.actual = actual;
            this.groupExtractor = groupExtractor;
            this.queueSupplier = queueSupplier;
            this.listener = listener;
            this.componentExtractor = componentExtractor;
            this.factory = factory;
            this.freeCapacity = maxInFlight;
        }

        @Override
        public void onSubscribe(Subscription s) {
            parent = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            Object group = groupExtractor.apply(t);
            AcknowledgementQueue queue = queuesByGroup.computeIfAbsent(group, this::newQueueForGroup);

            AcknowledgementQueue.InFlight inFlight =
                queue.add(componentExtractor.nativeAcknowledger(t), componentExtractor.nativeNacknowledger(t));
            listener.enqueued(group, 1);

            Runnable acknowledger = () -> postComplete(group, queue.complete(inFlight));
            Consumer<Throwable> nacknowedger = error -> postComplete(group, queue.completeExceptionally(inFlight, error));
            actual.onNext(factory.create(componentExtractor.value(t), acknowledger, nacknowedger));
        }

        @Override
        public void onError(Throwable t) {
            listener.close();
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            listener.close();
            actual.onComplete();
        }

        @Override
        public void request(long requested) {
            if (requested > 0L) {
                REQUEST_OUTSTANDING.addAndGet(this, requested);
                drainRequest();
            }
        }

        @Override
        public void cancel() {
            try {
                parent.cancel();
            } finally {
                listener.close();
            }
        }

        private AcknowledgementQueue newQueueForGroup(Object group) {
            listener.created(group);
            return queueSupplier.get();
        }

        private void postComplete(Object group, long drainedFromQueue) {
            if (drainedFromQueue > 0L) {
                listener.dequeued(group, drainedFromQueue);
                if (freeCapacity != Long.MAX_VALUE) {
                    FREE_CAPACITY.addAndGet(this, drainedFromQueue);
                    drainRequest();
                }
            }
        }

        private void drainRequest() {
            if (REQUESTS_IN_PROGRESS.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;
            do {
                long toRequest = Math.min(freeCapacity, requestOutstanding);

                if (toRequest > 0L) {
                    if (freeCapacity != Long.MAX_VALUE) {
                        FREE_CAPACITY.addAndGet(this, -toRequest);
                    }
                    REQUEST_OUTSTANDING.addAndGet(this, -toRequest);
                    parent.request(toRequest);
                }

                missed = REQUESTS_IN_PROGRESS.addAndGet(this, -missed);
            } while (missed != 0);
        }
    }
}
