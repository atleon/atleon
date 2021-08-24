package io.atleon.core;

import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class AbstractAlo<T> implements Alo<T> {

    @Override
    public boolean filter(Predicate<? super T> predicate, Consumer<? super Alo<T>> negativeConsumer) {
        boolean result = predicate.test(get());
        if (!result) {
            negativeConsumer.accept(this);
        }
        return result;
    }

    @Override
    public <R, C extends Collection<R>> Collection<Alo<R>>
    mapToMany(Function<? super T, ? extends C> mapper, Consumer<? super Alo<T>> emptyMappingConsumer) {
        C collection = mapper.apply(get());
        if (collection.isEmpty()) {
            emptyMappingConsumer.accept(this);
        }
        return collection.isEmpty()
            ? Collections.emptyList()
            : new AcknowledgingCollection<>(collection, getAcknowledger(), getNacknowledger(), createPropagator());
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return propagate(mapper.apply(get()), getAcknowledger(), getNacknowledger());
    }

    @Override
    public <R, P extends Publisher<R>> Publisher<Alo<R>> publish(Function<? super T, ? extends P> mapper) {
        return new AcknowledgingPublisher<>(mapper.apply(get()), getAcknowledger(), getNacknowledger(), createPropagator());
    }

    @Override
    public Alo<T> reduce(BinaryOperator<T> reducer, Alo<? extends T> other) {
        return propagate(reducer.apply(get(), other.get()),
            combineAcknowledgers(getAcknowledger(), other.getAcknowledger()),
            combineNacknowledgers(getNacknowledger(), other.getNacknowledger()));
    }

    @Override
    public final <R> Alo<R> propagate(R result, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        return this.<R>createPropagator().create(result, acknowledger, nacknowledger);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + get() + ")";
    }

    protected abstract <R> AloFactory<R> createPropagator();

    /**
     * Convenience method for combining Acknowledgers. Acknowledgers will be run in the same order
     * that they are provided
     */
    protected static Runnable combineAcknowledgers(Runnable acknowledger1, Runnable acknowledger2) {
        return () -> {
            acknowledger1.run();
            acknowledger2.run();
        };
    }

    /**
     * Convenience method for combining Nacknowledgers. Nacknowledgers will be run in the same
     * order that they are provided
     */
    protected static Consumer<Throwable> combineNacknowledgers(
        Consumer<? super Throwable> nacknowledger1,
        Consumer<? super Throwable> nacknowledger2) {
        return error -> {
            nacknowledger1.accept(error);
            nacknowledger2.accept(error);
        };
    }
}
