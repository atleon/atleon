package io.atleon.core;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Decorates data items with the notion of "acknowledgeability". An Alo's data item is not
 * considered fully processed until <i>either</i> its acknowledger or nacknowledger (negative
 * acknowledger) has been executed. Execution of the acknowledger signifies normal processing
 * completion of the correlated data item, while execution of the nacknowledger indicates abnormal,
 * unexpected, or otherwise exceptional termination of the processing of the correlated data item.
 * The Nacknowledger must always be exeucted with the {@link Throwable Throwable} that further
 * elaborates on the cause of exceptional processing termination.
 *
 * <p>Implementations of Alo should guarantee joint threadsafe idempotency of acknowledgement. In
 * other words, execution of either the acknowledger or nacknowledger must be threadsafe, and once
 * either is executed, further executions of either should result in no-ops. Implementations are
 * responsible for implementing how to propagate enough information with which to eventually
 * execute acknowledgement. Note that Alo extends {@link Contextual}, and implementations may
 * therefore propagate contextual information in addition to acknowledgement resources.
 *
 * <p>Acknowledgers and Nacknowledgers referenced by Alo implementations must be
 * <strong>safe</strong>. They <i>must not throw Exceptions.</i>
 *
 * @param <T> The type of data item contained in this Alo
 */
public interface Alo<T> extends Contextual {

    /**
     * Convenience method for executing an Alo's acknowledger. This is typically useful as a method
     * reference in higher order functions.
     *
     * @param alo The Alo to acknowledge
     */
    static void acknowledge(Alo<?> alo) {
        alo.getAcknowledger().run();
    }

    /**
     * Convenience method for executing an Alo's nacknowledger.
     *
     * @param alo   The Alo to nacknowledge
     * @param error The fatal error that resulted in termination of this message's processing
     */
    static void nacknowledge(Alo<?> alo, Throwable error) {
        alo.getNacknowledger().accept(error);
    }

    /**
     * Map this Alo's data item to another type, producing an Alo of the result type
     *
     * @param mapper Function to be applied to the data item
     * @param <R>    The resultant data item type
     * @return An Alo of the resultant type
     */
    default <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return this.<R>propagator().create(mapper.apply(get()), getAcknowledger(), getNacknowledger());
    }

    /**
     * Create an {@link AloFactory} that will be used to "fan in" the provided list of Alos which
     * reference the same type of data item as this one. Defaults to {@link Alo#propagator()}.
     *
     * @param alos The list of Alos to be "fanned in"
     * @return An AloFactory that will later be used to create a "fanned in" result
     */
    default AloFactory<List<T>> fanInPropagator(List<Alo<T>> alos) {
        return propagator();
    }

    /**
     * Create an {@link AloFactory} for some other data item (ordinarily derived from a
     * transformation of this Alo's data item) that propagates any relevant resources (like tracing
     * context). If there is nothing to propagate, this can typically be implemented by returning
     * {@link ComposedAlo#factory()}
     *
     * @param <R> The type of data item for which the returned AloFactory will wrap with Alo
     * @return An AloFactory used to create Alo implementations with any propagated data
     */
    <R> AloFactory<R> propagator();

    /**
     * Retrieve this Alo's data item
     */
    T get();

    /**
     * Retrieve this Alo's Acknowledger
     */
    Runnable getAcknowledger();

    /**
     * Retrieve this Alo's Nacknowledger
     */
    Consumer<? super Throwable> getNacknowledger();
}
