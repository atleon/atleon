package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

public class DefaultAloSenderResultSubscriber<T extends SenderResult> extends BaseSubscriber<Alo<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAloSenderResultSubscriber.class);

    @Override
    protected void hookOnNext(Alo<T> value) {
        T senderResult = value.get();
        if (senderResult.isFailure()) {
            Throwable cause = senderResult.failureCause().orElseGet(FailedSenderResultException::new);
            LOGGER.warn("SenderResult of type={} has failureCause={}", senderResult.getClass().getSimpleName(), cause);
            Alo.nacknowledge(value, cause);
        } else {
            Alo.acknowledge(value);
        }
    }

    private static final class FailedSenderResultException extends RuntimeException {

    }
}
