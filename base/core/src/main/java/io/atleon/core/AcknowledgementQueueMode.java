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
     * As {@link #STRICT}, but each completion is reported as a single upstream demand as soon as
     * the in-flight completes, rather than only when it is executed+drained. This frees downstream
     * backpressure capacity per logical completion instead of waiting for in-order execution, at
     * the cost of allowing more work to be requested ahead of that execution. Note that this mode
     * can result in unbounded memory usage if/when an in-flight at the head of the queue takes a
     * long time to complete (or never does), while elements continue to be processed.
     */
    EAGER(false, true),
    /**
     * Combines {@link #COMPACT} and {@link #EAGER}, where acknowledgements are allowed to coalesce
     * when multiple sequential acknowledgements are ready to be executed, and each completion is
     * reported as a single upstream demand as soon as the in-flight completes. This mode is
     * protected from unbounded memory usage, although the queue may hold up to twice as many
     * acknowledgements as the number of allowed in-flight elements.
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
