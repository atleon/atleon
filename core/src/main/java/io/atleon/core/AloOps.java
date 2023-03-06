package io.atleon.core;

import io.atleon.util.Throwing;
import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Common utility methods associated with operations on Alo and related components
 */
final class AloOps {

    private AloOps() {

    }

    public static <T, A extends Alo<T>> Predicate<A> filtering(
        Predicate<? super T> predicate,
        Consumer<? super A> negativeConsumer
    ) {
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

    public static <T, R> Function<Alo<T>, Collection<Alo<R>>> mappingToMany(
        Function<? super T, ? extends Collection<R>> mapper,
        Consumer<? super Alo<T>> emptyMappingConsumer
    ) {
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

    public static <T, R> Function<Alo<T>, Publisher<Alo<R>>> publishing(Function<? super T, ? extends Publisher<R>> mapper) {
        return alo -> {
            try {
                return AcknowledgingPublisher.fromAloPublisher(alo.map(mapper));
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T> Alo<List<T>> fanIn(List<Alo<T>> alos) {
        Alo<T> firstAlo = alos.get(0);
        if (alos.size() == 1) {
            return firstAlo.map(Collections::singletonList);
        } else {
            return firstAlo.<T>fanInPropagator(alos).create(
                alos.stream().map(Alo::get).collect(Collectors.toList()),
                combineAcknowledgers(alos.stream().map(Alo::getAcknowledger).collect(Collectors.toList())),
                combineNacknowledgers(alos.stream().map(Alo::getNacknowledger).collect(Collectors.toList()))
            );
        }
    }

    private static Runnable combineAcknowledgers(Iterable<? extends Runnable> acknowledgers) {
        return () -> acknowledgers.forEach(Runnable::run);
    }

    private static Consumer<? super Throwable> combineNacknowledgers(
        Iterable<? extends Consumer<? super Throwable>> nacknowledgers
    ) {
        return error -> nacknowledgers.forEach(nacknowledger -> nacknowledger.accept(error));
    }
}
