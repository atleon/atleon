package io.atleon.core;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface AloFactory<T> {

    static <T> Function<List<Alo<T>>, Alo<List<T>>>
    listInverting(Function<? super List<Alo<T>>, ? extends AloFactory<List<T>>> listToAloFactory) {
        return list -> invertList(list, listToAloFactory.apply(list));
    }

    static <T> Alo<List<T>> invertList(List<Alo<T>> list, AloFactory<List<T>> factory) {
        List<T> values = list.stream().map(Alo::get).collect(Collectors.toList());
        Collection<Runnable> acknowledgers = list.stream().map(Alo::getAcknowledger).collect(Collectors.toList());
        Collection<Consumer<? super Throwable>> nacknowledgers = list.stream().map(Alo::getNacknowledger).collect(Collectors.toList());
        return factory.create(values,
            () -> acknowledgers.forEach(Runnable::run),
            error -> nacknowledgers.forEach(nacknowledger -> nacknowledger.accept(error)));
    }

    Alo<T> create(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowedger);
}
