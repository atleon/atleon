package io.atleon.core;

import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Alo<T> {

    static void acknowledge(Alo<?> alo) {
        alo.getAcknowledger().run();
    }

    static void nacknowledge(Alo<?> alo, Throwable error) {
        alo.getNacknowledger().accept(error);
    }

    /**
     * Test this Alo's data item against a supplied predicate, and execute the supplied Consumer
     * with this Alo if the Predicate tests negative (i.e. false)
     *
     * @param predicate        The Predicate to test this Alo's data item against
     * @param negativeConsumer Consumer to accept this Alo if Predicate returns false
     * @return The result of testing this Alo's data item against supplied Predicate
     */
    boolean filter(Predicate<? super T> predicate, Consumer<? super Alo<T>> negativeConsumer);

    /**
     * Map this Alo's data item to another type, producing an Alo of the result type
     *
     * @param mapper Function to be applied to the data item
     * @param <R>    The resultant data item type
     * @return An Alo of the resultant type
     */
    <R> Alo<R> map(Function<? super T, ? extends R> mapper);

    /**
     * Map this Alo in to a Collection of Acknowledgeables using the supplied Mapper. The Mapper
     * will be applied to the data item, and the Collection of results will themselves each be
     * wrapped as Alo. The originating acknowledgement should not be executed until all resultant
     * items are (n)acknowledged. The supplied Consumer will be passed this Alo if the mapping
     * produces an empty result Collection.
     *
     * @param mapper               Function that produces 0..n results from contained data item
     * @param emptyMappingConsumer Consumer to accept this Alo if mapper returns empty
     * @param <R>                  The type of elements in the resultant Collection
     * @param <C>                  The type of Collection produced by the mapper
     * @return The mapped collection of results where each item is Alo
     */
    <R, C extends Collection<R>> Collection<Alo<R>>
    mapToMany(Function<? super T, ? extends C> mapper, Consumer<? super Alo<T>> emptyMappingConsumer);

    /**
     * Map this Alo to a {@link org.reactivestreams.Publisher Publisher} of results. This Alo's
     * (n)acknowledgement should be executed once one of the following is true:
     * - The resultant Publisher has been terminated or canceled AND all emitted results have
     *   been acknowledged
     * - At least one has been nacknowledged
     *
     * @param mapper Transforms this Alo's data item in to a Publisher of results
     * @param <R>    The type of results to be published
     * @param <P>    The type of Publisher to emit results
     * @return a Publisher of results
     */
    <R, P extends Publisher<R>> Publisher<Alo<R>> publish(Function<? super T, ? extends P> mapper);

    /**
     * Apply a reduction on this Alo with another, producing a reduced Alo
     *
     * @param reducer The reduction to apply
     * @param other   The other Alo to apply reduction with
     * @return a reduced Alo
     */
    Alo<T> reduce(BinaryOperator<T> reducer, Alo<? extends T> other);

    /**
     * Consume this Alo's Data item with the provided Consumer, followed by consumption of this Alo
     * (i.e. Alo::acknowledge) upon not-exceptional completion of the data item consumer
     *
     * @param consumer The Consumer to accept this Alo's Data item
     * @param andThen  A Consumer to accept this Alo after consumption of data item
     */
    void consume(Consumer<? super T> consumer, Consumer<? super Alo<T>> andThen);

    /**
     * Method for allowing propagation of any metadata (like tracing context) from this
     * Alo to an Alo composed with the supplied Acknowledger and Nacknowledger
     *
     * @param result An Alo backed by the supplied Acknowledger and Nacknowledger
     * @param <R>    The type of data item being propagated
     * @return An Alo of the resultant type
     */
    <R> Alo<R> propagate(R result, Runnable acknowledger, Consumer<? super Throwable> nacknowledger);

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
