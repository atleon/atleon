package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

final class AloErrorDelegatingMapper<T> implements Function<Alo<T>, Alo<T>> {

    private final BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegate;

    AloErrorDelegatingMapper(BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Alo<T> apply(Alo<T> alo) {
        Consumer<Throwable> nacknowledger = buildNacknowledger(alo.get(), alo.getAcknowledger(), alo.getNacknowledger());
        return alo.<T>propagator().create(alo.get(), alo.getAcknowledger(), nacknowledger);
    }

    private Consumer<Throwable> buildNacknowledger(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        return error -> delegateError(t, error).subscribe(__ -> {}, nacknowledger, acknowledger);
    }

    private Mono<Void> delegateError(T t, Throwable error) {
        try {
            return Mono.when(delegate.apply(t, error))
                .onErrorMap(delegateError -> consolidateErrors(error, delegateError));
        } catch (Throwable delegateError) {
            return Mono.error(consolidateErrors(error, delegateError));
        }
    }

    private static Throwable consolidateErrors(Throwable originalError, Throwable delegateError) {
        if (originalError != delegateError) {
            originalError.addSuppressed(delegateError);
        }
        return originalError;
    }
}
