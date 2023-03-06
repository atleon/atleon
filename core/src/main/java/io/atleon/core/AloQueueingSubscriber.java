package io.atleon.core;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Operator used in Publishers of Alo elements that enables "queueing" of acknowledgement such that
 * acknowledgement order is preserved independently of data processing completion.
 *
 * @param <T> The type of data item contained in emitted Alo elements
 * @param <A> The type of Alo emitted
 */
final class AloQueueingSubscriber<T, A extends Alo<T>> implements Subscriber<A>, Subscription {

    private static final AtomicLongFieldUpdater<AloQueueingSubscriber> FREE_CAPACITY =
        AtomicLongFieldUpdater.newUpdater(AloQueueingSubscriber.class, "freeCapacity");

    private static final AtomicLongFieldUpdater<AloQueueingSubscriber> REQUEST_OUTSTANDING =
        AtomicLongFieldUpdater.newUpdater(AloQueueingSubscriber.class, "requestOutstanding");

    private static final AtomicIntegerFieldUpdater<AloQueueingSubscriber> REQUESTS_IN_PROGRESS =
        AtomicIntegerFieldUpdater.newUpdater(AloQueueingSubscriber.class, "requestsInProgress");

    private final Map<Object, AcknowledgementQueue> queuesByGroup = new ConcurrentHashMap<>();

    private final Subscriber<? super Alo<T>> actual;

    private final Function<T, ?> groupExtractor;

    private final Supplier<? extends AcknowledgementQueue> queueSupplier;

    private Subscription parent;

    private volatile long freeCapacity;

    private volatile long requestOutstanding;

    private volatile int requestsInProgress;

    AloQueueingSubscriber(
        Subscriber<? super Alo<T>> actual,
        Function<T, ?> groupExtractor,
        Supplier<? extends AcknowledgementQueue> queueSupplier,
        long maxInFlight) {
        this.actual = actual;
        this.queueSupplier = queueSupplier;
        this.groupExtractor = groupExtractor;
        this.freeCapacity = maxInFlight;
    }

    @Override
    public void onSubscribe(Subscription s) {
        parent = s;
        actual.onSubscribe(this);
    }

    @Override
    public void onNext(A a) {
        AcknowledgementQueue queue = queuesByGroup.computeIfAbsent(groupExtractor.apply(a.get()), __ -> queueSupplier.get());

        AcknowledgementQueue.InFlight inFlight = queue.add(a.getAcknowledger(), a.getNacknowledger());

        Runnable acknowledger = () -> postComplete(queue.complete(inFlight));
        Consumer<Throwable> nacknowedger = error -> postComplete(queue.completeExceptionally(inFlight, error));
        actual.onNext(a.<T>propagator().create(a.get(), acknowledger, nacknowedger));
    }

    @Override
    public void onError(Throwable t) {
        actual.onError(t);
    }

    @Override
    public void onComplete() {
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
        parent.cancel();
    }

    private void postComplete(long drainedFromQueue) {
        if (freeCapacity != Long.MAX_VALUE && drainedFromQueue > 0L) {
            FREE_CAPACITY.addAndGet(this, drainedFromQueue);
            drainRequest();
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
