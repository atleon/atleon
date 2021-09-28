package io.atleon.core;

import io.atleon.util.Throwing;
import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Common utility methods associated with operations on Alo and components
 */
final class AloOps {

    private AloOps() {

    }

    public static <T, A extends Alo<T>> Predicate<A>
    filtering(Predicate<? super T> predicate, Consumer<? super A> negativeConsumer) {
        return alo -> {
            try {
                boolean result = predicate.test(alo.get());
                if (!result) {
                    negativeConsumer.accept(alo);
                }
                return result;
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, R> Function<Alo<T>, Alo<R>> mapping(Function<? super T, ? extends R> mapper) {
        return alo -> {
            try {
                return alo.map(mapper);
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, R> Function<Alo<T>, Collection<Alo<R>>>
    mappingToMany(Function<? super T, ? extends Collection<R>> mapper, Consumer<? super Alo<T>> emptyMappingConsumer) {
        return alo -> {
            try {
                Alo<Collection<R>> result = alo.map(mapper);
                if (result.get().isEmpty()) {
                    emptyMappingConsumer.accept(alo);
                    return Collections.emptyList();
                } else {
                    return AcknowledgingCollection.fromNonEmptyAloCollection(result);
                }
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, R> Function<Alo<T>, Publisher<Alo<R>>>
    publishing(Function<? super T, ? extends Publisher<R>> mapper) {
        return alo -> {
            try {
                return AcknowledgingPublisher.fromAloPublisher(alo.map(mapper));
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T> BinaryOperator<Alo<T>> reducing(BinaryOperator<T> reducer) {
        return (alo1, alo2) -> {
            try {
                return alo1.reduce(reducer, alo2);
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
