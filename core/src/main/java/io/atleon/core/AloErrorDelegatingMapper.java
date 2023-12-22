package io.atleon.core;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

final class AloErrorDelegatingMapper<T> implements Function<Alo<T>, Alo<T>> {

    private final BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegator;

    AloErrorDelegatingMapper(BiFunction<? super T, ? super Throwable, ? extends Publisher<?>> delegator) {
        this.delegator = delegator;
    }

    @Override
    public Alo<T> apply(Alo<T> alo) {
        return alo.<T>propagator().create(alo.get(), alo.getAcknowledger(), error -> delegateAloError(alo, error));
    }

    private void delegateAloError(Alo<T> alo, Throwable error) {
        alo.runInContext(() ->
            delegateError(alo.get(), error).subscribe(__ -> {}, alo.getNacknowledger(), alo.getAcknowledger())
        );
    }

    private Mono<Void> delegateError(T t, Throwable error) {
        try {
            return Mono.when(delegator.apply(t, error))
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
