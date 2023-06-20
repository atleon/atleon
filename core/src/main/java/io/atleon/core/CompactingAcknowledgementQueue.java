package io.atleon.core;

import java.util.function.Consumer;
import java.util.function.Function;

public class CompactingAcknowledgementQueue extends AcknowledgementQueue {

    private final CompactingQueue<InFlight> compactingQueue;

    private CompactingAcknowledgementQueue() {
        this(new ConcurrentCompactingQueue<>(i -> !i.isInProcess()));
    }

    private CompactingAcknowledgementQueue(final CompactingQueue<InFlight> queue) {
        super(queue);
        this.compactingQueue = queue;
    }

    public static AcknowledgementQueue create() {
        return new CompactingAcknowledgementQueue();
    }

    @Override
    protected boolean complete(InFlight inFlight, Function<InFlight, Boolean> completer) {
        return completer.apply(inFlight);
    }

    @Override
    public InFlight add(Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        CompactingInFlight inFlight = new CompactingInFlight(acknowledger, nacknowledger);
        inFlight.node = compactingQueue.addItem(inFlight);
        return inFlight;
    }

    static class CompactingInFlight extends InFlight {

        private CompactingQueue.Node<InFlight> node;

        CompactingInFlight(Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
            super(acknowledger, nacknowledger);
        }

        @Override
        protected boolean complete() {
            tryCompact();
            return super.complete();
        }

        @Override
        protected boolean completeExceptionally(Throwable error) {
            tryCompact();
            return super.completeExceptionally(error);
        }

        private void tryCompact() {
            if (node != null) {
                for (final InFlight f : node.tryCompact()) {
                    this.weight += f.weight;
                }
            }
        }

    }
}
