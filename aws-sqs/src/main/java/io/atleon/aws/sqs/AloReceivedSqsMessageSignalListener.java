package io.atleon.aws.sqs;

import io.atleon.core.AloSignalListener;

/**
 * Interface through which side effects on {@link reactor.core.publisher.Signal}s emitted from
 * Reactor Publishers of {@link io.atleon.core.Alo}s referencing {@link ReceivedSqsMessage}s can be
 * implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.aws.sqs.AloReceivedSqsMessageSignalListener} in your
 * project's resource directory.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public interface AloReceivedSqsMessageSignalListener<T> extends AloSignalListener<ReceivedSqsMessage<T>> {

    /**
     * This parameter will be populated during configuration to let the decorator know what the URL
     * is of the queue being consumed from.
     */
    String QUEUE_URL_CONFIG = "alo.signal.listener.sqs.queue.url";
}
