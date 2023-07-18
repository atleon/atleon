package io.atleon.core;

/**
 * Controls the behavior of queuing in {@link AcknowledgementQueue}
 */
public enum AcknowledgementQueueMode {
    /**
     * Every queued acknowledgement will be executed in order
     */
    STRICT,
    /**
     * Acknowledgements may be skipped when multiple sequential acknowledgements are ready to be
     * executed
     */
    COMPACT
}
