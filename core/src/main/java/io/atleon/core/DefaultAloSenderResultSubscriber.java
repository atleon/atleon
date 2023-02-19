package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

/**
 * Default Subscriber of {@link Alo} items referencing {@link SenderResult}s.
 *
 * @param <T> The type of results to subscribed to
 */
public class DefaultAloSenderResultSubscriber<T extends SenderResult> extends BaseSubscriber<Alo<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAloSenderResultSubscriber.class);

    @Override
    protected final void hookOnNext(Alo<T> value) {
        T senderResult = value.get();
        if (shouldAcknowledge(senderResult)) {
            Alo.acknowledge(value);
        } else {
            hookBeforeNacknowledge(senderResult);
            Alo.nacknowledge(value, senderResult.failureCause().orElseGet(() -> new SenderFailureException(senderResult)));
        }
    }

    /**
     * Overridable method for determining whether a {@link SenderResult} should be positively
     * acknowledged. Extensions may want to override this method when a failure resulting from
     * sending a message may be ignorable. By default, returns true if the result is not a failure.
     *
     * @param senderResult The result of sending a message
     * @return True if the originating message should be acknowledged
     */
    protected boolean shouldAcknowledge(T senderResult) {
        return !senderResult.isFailure();
    }

    /**
     * Hook for negative acknowledgement of a {@link SenderResult}. Default implementation logs the
     * result type and failure cause.
     *
     * @param senderResult The result that will be negatively acknowledged
     */
    protected void hookBeforeNacknowledge(T senderResult) {
        Throwable failureCause = senderResult.failureCause().orElse(null);
        LOGGER.warn("SenderResult of type={} has failureCause={}", senderResult.getClass().getSimpleName(), failureCause);
    }

    private static final class SenderFailureException extends RuntimeException {

        private SenderFailureException(SenderResult senderResult) {
            super("SenderResult is a failure: " + senderResult);
        }
    }
}
