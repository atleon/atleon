package io.atleon.core;

import java.util.Iterator;
import java.util.function.Function;

final class MultipleAcknowledgementQueue extends AcknowledgementQueue {

    private MultipleAcknowledgementQueue() {

    }

    public static AcknowledgementQueue create() {
        return new MultipleAcknowledgementQueue();
    }

    //TODO This method may possibly be made more performant, but would be non-trivial to do so. At
    // ToW, performance optimization was not absolutely necessary since multiple acknowledgement,
    // particularly bounded multiple acknowledgement, is/was a rare use case; Even so, typical
    // multiple acknowledgement use cases have acknowledgement externally synchronized (i.e. non
    // concurrent), which means there is low probability of Thread contention here, under which
    // condition there is little overhead incurred by the synchronization block. Furthermore,
    // multiple acknowledgement inherently acknowledges at a frequency less-than-or-equal-to the
    // frequency of data emissions (in fact, usually far less than emission frequency), resulting
    // in still less impact by the synchronization block
    @Override
    protected boolean complete(InFlight lastInFlight, Function<InFlight, Boolean> completer) {
        boolean anyCompleted = false;
        synchronized (queue) {
            Iterator<InFlight> iterator = queue.iterator();
            while (lastInFlight.isInProcess() && iterator.hasNext()) {
                anyCompleted |= completer.apply(iterator.next());
            }
        }
        return anyCompleted;
    }
}
