package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

import java.util.Optional;

public class DefaultAloSenderResultSubscriber<T extends SenderResult> extends BaseSubscriber<Alo<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAloSenderResultSubscriber.class);

    @Override
    protected void hookOnNext(Alo<T> value) {
        T senderResult = value.get();
        Optional<Throwable> failure = senderResult.failureCause();
        if (failure.isPresent()) {
            LOGGER.warn("SenderResult type={} has failure={}", senderResult.getClass().getSimpleName(), failure.get());
            Alo.nacknowledge(value, failure.get());
        } else {
            Alo.acknowledge(value);
        }
    }
}
