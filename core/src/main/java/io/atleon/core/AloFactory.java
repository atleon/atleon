package io.atleon.core;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Factory for creating implementations of {@link Alo}
 *
 * @param <T>
 */
@FunctionalInterface
public interface AloFactory<T> {

    static <T> Alo<List<T>> invertList(List<Alo<T>> list, AloFactory<List<T>> factory) {
        List<T> values = list.stream().map(Alo::get).collect(Collectors.toList());
        Runnable acknowledger = list.stream()
            .map(Alo::getAcknowledger)
            .collect(Collectors.collectingAndThen(Collectors.toList(), AloOps::combineAcknowledgers));
        Consumer<? super Throwable> nacknowledger = list.stream()
            .map(Alo::getNacknowledger)
            .collect(Collectors.collectingAndThen(Collectors.toList(), AloOps::combineNacknowledgers));
        return factory.create(values, acknowledger, nacknowledger);
    }

    Alo<T> create(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowedger);
}
