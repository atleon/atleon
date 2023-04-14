package io.atleon.core;

import io.atleon.util.Throwing;
import reactor.core.publisher.SynchronousSink;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
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

    public static <T, A extends Alo<T>> Predicate<A>
    filtering(Predicate<? super T> predicate, Consumer<? super A> negativeConsumer) {
        return alo -> {
            boolean result;
            try {
                result = predicate.test(alo.get());
            } catch (Throwable error) {
                throw Throwing.propagate(error);
            }

            if (!result) {
                negativeConsumer.accept(alo);
            }
            return result;
        };
    }

    public static <T, R> Function<Alo<T>, Alo<R>>
    mapping(Function<? super T, ? extends R> mapper) {
        return alo -> {
            try {
                return Objects.requireNonNull(alo.map(mapper), "Alo implementation returned null mapping");
            } catch (Throwable error) {
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, R> BiConsumer<Alo<T>, SynchronousSink<Alo<R>>>
    mappingPresentHandler(Function<? super T, Optional<? extends R>> mapper, Consumer<? super Alo<T>> absentConsumer) {
        return (alo, sink) -> {
            Alo<Optional<? extends R>> result;
            try {
                result = Objects.requireNonNull(alo.map(mapper), "Alo implementation returned null mapping");
            } catch (Throwable error) {
                throw Throwing.propagate(error);
            }

            if (result.get().isPresent()) {
                sink.next(PresentAlo.wrap(result));
            } else {
                absentConsumer.accept(alo);
            }
        };
    }

    public static <T, R> BiConsumer<Alo<T>, SynchronousSink<Alo<Collection<R>>>>
    mappingToManyHandler(Function<? super T, ? extends Collection<R>> mapper, Consumer<? super Alo<T>> emptyMappingConsumer) {
        return (alo, sink) -> {
            Alo<Collection<R>> result;
            try {
                result = Objects.requireNonNull(alo.map(mapper), "Alo implementation returned null mapping");
            } catch (Throwable error) {
                throw Throwing.propagate(error);
            }

            if (result.get().isEmpty()) {
                emptyMappingConsumer.accept(alo);
            } else {
                sink.next(result);
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

    private static Consumer<? super Throwable>
    combineNacknowledgers(Iterable<? extends Consumer<? super Throwable>> nacknowledgers) {
        return error -> nacknowledgers.forEach(nacknowledger -> nacknowledger.accept(error));
    }
}
