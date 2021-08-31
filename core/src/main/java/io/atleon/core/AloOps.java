package io.atleon.core;

import io.atleon.util.Throwing;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Common utility methods associated with operations on Alo and components
 */
public final class AloOps {

    private AloOps() {

    }

    public static <T, A extends Alo<T>> Predicate<A> wrapFilter(Predicate<? super A> filter) {
        return alo -> {
            try {
                return filter.test(alo);
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, A extends Alo<T>, R> Function<A, R> wrapMapper(Function<? super A, ? extends R> mapper) {
        return alo -> {
            try {
                return mapper.apply(alo);
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, A extends Alo<T>> BiFunction<A, A, A> wrapAggregator(BiFunction<A, A, A> aggregator) {
        return (alo1, alo2) -> {
            try {
                return aggregator.apply(alo1, alo2);
            } catch (Throwable error) {
                Alo.nacknowledge(alo1, error);
                Alo.nacknowledge(alo2, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static Runnable combineAcknowledgers(Iterable<? extends Runnable> acknowledgers) {
        return () -> acknowledgers.forEach(Runnable::run);
    }

    public static Consumer<? super Throwable>
    combineNacknowledgers(Iterable<? extends Consumer<? super Throwable>> nacknowledgers) {
        return error -> nacknowledgers.forEach(nacknowledger -> nacknowledger.accept(error));
    }

    /**
     * Convenience method for combining Acknowledgers. Acknowledgers will be run in the same order
     * that they are provided
     */
    public static Runnable combineAcknowledgers(Runnable acknowledger1, Runnable acknowledger2) {
        return () -> {
            acknowledger1.run();
            acknowledger2.run();
        };
    }

    /**
     * Convenience method for combining Nacknowledgers. Nacknowledgers will be run in the same
     * order that they are provided
     */
    public static Consumer<Throwable> combineNacknowledgers(
        Consumer<? super Throwable> nacknowledger1,
        Consumer<? super Throwable> nacknowledger2) {
        return error -> {
            nacknowledger1.accept(error);
            nacknowledger2.accept(error);
        };
    }
}
