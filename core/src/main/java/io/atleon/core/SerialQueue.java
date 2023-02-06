package io.atleon.core;

import reactor.core.publisher.Sinks;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

/**
 * A thread-safe Queue of tasks which must be executed serially. This provides non-blocking
 * thread-safe fan-in while ensuring at most one thread drains the queued tasks. This is useful
 * when a certain action on a given resource must be serialized, but requests to that resource may
 * come concurrently from multiple threads.
 *
 * @param <T> The type of elements held by this Queue
 */
public final class SerialQueue<T> {

    private static final AtomicIntegerFieldUpdater<SerialQueue> DRAINS_IN_PROGRESS =
        AtomicIntegerFieldUpdater.newUpdater(SerialQueue.class, "drainsInProgress");

    private volatile int drainsInProgress;

    private final Queue<T> queue = new ConcurrentLinkedQueue<>();

    private final Consumer<? super T> drain;

    private SerialQueue(Consumer<? super T> drain) {
        this.drain = drain;
    }

    /**
     * Creates a {@link SerialQueue} that wraps the emissions of next items on a
     * {@link Sinks.Many}. Next emissions will fail fast if any of the sink's emissions are invoked
     * externally and not serialized with the created Queue.
     *
     * @param sink The sink into which queued items will be emitted
     * @param <T> The type of items emitted in to the sink
     * @return A new DrainableQueue
     */
    public static <T> SerialQueue<T> onEmitNext(Sinks.Many<T> sink) {
        return new SerialQueue<>(t -> sink.emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST));
    }

    /**
     * Adds an item to this Queue and attempts to drain it. If another thread is already draining,
     * that thread may be the one to drain this item, rather than the calling thread.
     *
     * @param t The item to add
     */
    public void addAndDrain(T t) {
        queue.add(t);
        drain();
    }

    private void drain() {
        if (DRAINS_IN_PROGRESS.getAndIncrement(this) != 0) {
            return;
        }

        int missed = 1;
        do {
            while (!queue.isEmpty()) {
                drain.accept(queue.remove());
            }
            missed = DRAINS_IN_PROGRESS.addAndGet(this, -missed);
        } while (missed != 0);
    }
}
