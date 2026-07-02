package io.atleon.core;

/**
 * Controls the behavior of queuing in {@link AcknowledgementQueue} along two independent axes:
 * <ul>
 * <li><b>Execution Compaction:</b> By default, all completed in-flights have their associated
 * acknowledgements executed. This can rather be configured to enable "compaction", where
 * consecutively completed in-flights may have execution skipped in favor of executing the latest
 * completed in-flight. Compaction optimizes memory usage by allowing consecutively completed
 * in-flights to coalesce into a single acknowledgement execution. This is useful (for example)
 * when consumption is based on monotonically-increasing offsets.</li>
 * <li><b>Upstream Demand Timing:</b> By default, demand is only signaled on in-flight completion
 * when its acknowledgement is executed and it (as well as any others that are executed) is drained
 * from the queue. This can rather be configured to happen immediately on completion (EAGER), such
 * as to eagerly request more elements from upstream as soon as processing an element has
 * completed, and (possibly) before its acknowledgement has executed.</li>
 * </ul>
 */
public enum AcknowledgementQueueMode {
    /**
     * Every queued acknowledgement will be executed in order. Completion reports the number of
     * elements actually drained from the queue, which may be zero when completion is blocked
     * behind earlier in-flight acknowledgements.
     */
    STRICT(false, false),
    /**
     * Acknowledgement executions may be skipped when multiple sequential acknowledgements are
     * ready to be executed. Completion <i>may</i> signal demand if/when acknowledgements are
     * coalesced, which may not coincide with any acknowledgement execution.
     */
    COMPACT(true, false),
    /**
     * As {@link #COMPACT}, where acknowledgements are allowed to coalesce when multiple sequential
     * acknowledgements are ready to be executed, but each completion is reported as a single
     * upstream demand as soon as each in-flight completes.
     */
    COMPACT_EAGER(true, true);

    private final boolean compact;

    private final boolean demandEager;

    AcknowledgementQueueMode(boolean compact, boolean demandEager) {
        this.compact = compact;
        this.demandEager = demandEager;
    }

    public boolean isCompact() {
        return compact;
    }

    boolean isDemandEager() {
        return demandEager;
    }
}
