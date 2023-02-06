package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

/**
 * Acknowledges emitted {@link Alo} items, while logging a warning about doing so.
 *
 * @param <T> The type of payloads referenced by received Alo items
 */
public final class WarningAloSubscriber<T> extends BaseSubscriber<Alo<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarningAloSubscriber.class);

    WarningAloSubscriber() {

    }

    @Override
    protected void hookOnNext(Alo<T> alo) {
        LOGGER.warn("Alo item is being acknowledged by default. While this may be reasonable in " +
            "certain cases, it is more likely an error in the Alo Publisher pipeline. Your " +
            "pipeline should either use a specific Subscriber for the emitted Alo type or " +
            "explicitly subscribe with Alo::acknowledge. alo={}", alo);
        Alo.acknowledge(alo);
    }
}
