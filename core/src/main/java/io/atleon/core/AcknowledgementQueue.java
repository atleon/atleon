package io.atleon.core;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A single-producer, multiple-consumer (SPMC) Queue that manages order-of-execution of in-flight
 * Acknowledgements. Addition is done by providing callbacks for successful and erroneous
 * completion, and is thread-compatible, but not thread-safe. Addition produces an {@link InFlight}
 * receipt handle which must be passed back to this queue in order complete it. Completion is
 * thread-safe, and may or may not result in the execution of the associated {@link InFlight},
 * along with other {@link InFlight}s that have come before it.
 */
public final class AcknowledgementQueue {

    private static final AtomicReferenceFieldUpdater<AcknowledgementQueue, InFlight> TAIL =
        AtomicReferenceFieldUpdater.newUpdater(AcknowledgementQueue.class, InFlight.class, "tail");

    private static final AtomicIntegerFieldUpdater<AcknowledgementQueue> DRAINS_IN_PROGRESS =
        AtomicIntegerFieldUpdater.newUpdater(AcknowledgementQueue.class, "drainsInProgress");

    private final AcknowledgementQueueMode mode;

    private final Queue<InFlight> drainQueue = new ConcurrentLinkedQueue<>();

    private volatile InFlight tail;

    private volatile int drainsInProgress;

    private AcknowledgementQueue(AcknowledgementQueueMode mode) {
        this.mode = mode;
    }

    public static AcknowledgementQueue create(AcknowledgementQueueMode mode) {
        return new AcknowledgementQueue(mode);
    }

    /**
     * Append an In-Flight Acknowledgement to the Queue backed by the following Acknowledger and
     * Nacknowledger
     *
     * @return The In-Flight Acknowledgement to be completed on this Queue in the Future
     */
    public InFlight add(Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        for (; ; ) {
            InFlight observedTail = tail;
            InFlight previous = observedTail == null || observedTail.isSevered() ? null : observedTail;
            InFlight newTail = new InFlight(acknowledger, nacknowledger, previous);
            if (previous == null || previous.casNext(null, newTail)) { // Account for possible race with sever
                TAIL.set(this, newTail);
                return newTail;
            }
        }
    }

    /**
     * Complete an In-Flight Acknowledgement in this Queue
     *
     * @return The number of elements drained from this Queue due to completion of Acknowledgement
     */
    public long complete(InFlight toComplete) {
        return complete(toComplete, InFlight::complete) ? drainFrom(toComplete) : 0L;
    }

    /**
     * Exceptionally complete an In-Flight Acknowledgement in this Queue
     *
     * @return The number of elements drained from this Queue due to completion of Acknowledgement
     */
    public long completeExceptionally(InFlight toComplete, Throwable error) {
        return complete(toComplete, inFlight -> inFlight.completeExceptionally(error)) ? drainFrom(toComplete) : 0L;
    }

    private boolean complete(InFlight inFlight, Predicate<InFlight> completer) {
        return completer.test(inFlight);
    }

    private long drainFrom(InFlight completed) {
        drainQueue.add(completed);
        return drain();
    }

    private long drain() {
        if (DRAINS_IN_PROGRESS.getAndIncrement(this) != 0) {
            return 0L;
        }

        long drained = 0L;
        int missed = 1;
        do {
            for (InFlight completed = drainQueue.poll(); completed != null; completed = drainQueue.poll()) {
                if (!completed.isSevered()) {
                    drained += completed.isHead() ? drainHead(completed) : drainNonHead(completed);
                }
            }

            InFlight observedTail = tail;
            if (observedTail != null && observedTail.isSevered()) {
                TAIL.compareAndSet(this, observedTail, null); // Fine if this fails
            }

            missed = DRAINS_IN_PROGRESS.addAndGet(this, -missed);
        } while (missed != 0);

        return drained;
    }

    private long drainHead(InFlight head) {
        long drained = 0L;
        while (head != null && !head.isInProcess()) {
            InFlight next = head.sever();
            head.execute();
            drained++;
            head = next;
        }
        return drained;
    }

    private long drainNonHead(InFlight nonHead) {
        return mode == AcknowledgementQueueMode.COMPACT ? nonHead.tryCompact() : 0L;
    }

    public static final class InFlight {

        private enum State {IN_PROCESS, COMPLETED, EXECUTED}

        private static final AtomicReferenceFieldUpdater<InFlight, InFlight> NEXT =
            AtomicReferenceFieldUpdater.newUpdater(InFlight.class, InFlight.class, "next");

        private static final AtomicReferenceFieldUpdater<InFlight, State> STATE =
            AtomicReferenceFieldUpdater.newUpdater(InFlight.class, State.class, "state");

        private static final AtomicReferenceFieldUpdater<InFlight, Throwable> ERROR =
            AtomicReferenceFieldUpdater.newUpdater(InFlight.class, Throwable.class, "error");

        private final Runnable acknowledger;

        private final Consumer<? super Throwable> nacknowledger;

        private volatile InFlight previous;

        private volatile InFlight next;

        private volatile State state = State.IN_PROCESS;

        private volatile Throwable error;

        private InFlight(Runnable acknowledger, Consumer<? super Throwable> nacknowledger, InFlight previous) {
            this.acknowledger = acknowledger;
            this.nacknowledger = nacknowledger;
            this.previous = previous;
        }

        private long tryCompact() {
            long compacted = 0L;
            if (!isCompletedWithoutError()) {
                return compacted;
            }

            InFlight left = previous;
            if (left.isCompletedWithoutError() && !left.isHead()) { // Don't sever head; Its completion must cause execution
                left.sever(); // Need to sever left since it could be queued for draining and should then result in no-op
                left = left.previous;
                left.next.previous = null; // Enable efficient GC
                compacted++;
            }

            InFlight right = this;
            if (right.next != null && right.next.isCompletedWithoutError()) {
                // No need to sever right since we know it (this) can't be completed again
                right.previous = null; // Enable efficient GC
                right = right.next;
                NEXT.lazySet(right.previous, null); // Enable efficient GC
                compacted++;
            }

            if (compacted != 0L) { // Only modify pointers if we actually compacted
                NEXT.set(left, right);
                right.previous = left;
            }

            return compacted;
        }

        private InFlight sever() {
            InFlight observedNext = next;
            while (!casNext(observedNext, this)) { // Account for possible race with addition
                observedNext = next;
            }

            if (observedNext != null) {
                observedNext.previous = null;
            }

            return observedNext;
        }

        private boolean isHead() {
            return previous == null;
        }

        private boolean isSevered() {
            return next == this;
        }

        private boolean casNext(InFlight expect, InFlight next) {
            return NEXT.compareAndSet(this, expect, next);
        }

        private boolean isInProcess() {
            return state == State.IN_PROCESS;
        }

        private boolean isCompletedWithoutError() {
            return state == State.COMPLETED && error == null; // Ordering is important: State may be set after reading error
        }

        private boolean completeExceptionally(Throwable error) {
            return state == State.IN_PROCESS && ERROR.compareAndSet(this, null, error) && complete();
        }

        private boolean complete() {
            return STATE.compareAndSet(this, State.IN_PROCESS, State.COMPLETED);
        }

        private void execute() {
            if (STATE.getAndSet(this, State.EXECUTED) == State.EXECUTED) {
                return;
            }

            if (error == null) {
                acknowledger.run();
            } else {
                nacknowledger.accept(error);
            }
        }
    }
}
