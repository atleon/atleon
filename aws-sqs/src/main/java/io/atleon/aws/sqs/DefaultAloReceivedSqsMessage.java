package io.atleon.aws.sqs;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;

import java.util.function.Consumer;

/**
 * Default implementation of an {@link Alo} wrapper for a {@link ReceivedSqsMessage}
 *
 * @param <T> The (deserialized) type of body referenced by the Message wrapped by this {@link Alo}
 */
public final class DefaultAloReceivedSqsMessage<T> implements Alo<ReceivedSqsMessage<T>> {

    private final ReceivedSqsMessage<T> message;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public DefaultAloReceivedSqsMessage(
        ReceivedSqsMessage<T> message,
        Runnable acknowledger,
        Consumer<? super Throwable> nacknowledger
    ) {
        this.message = message;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return ComposedAlo::new;
    }

    @Override
    public ReceivedSqsMessage<T> get() {
        return message;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }
}
