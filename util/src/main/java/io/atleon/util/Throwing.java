package io.atleon.util;

public final class Throwing {

    private Throwing() {

    }

    public static RuntimeException propagate(Throwable throwable) {
        return propagate(throwable, RuntimeException::new);
    }

    public static RuntimeException propagate(Throwable throwable, java.util.function.Function<? super Throwable, ? extends RuntimeException> runtimeExceptionWrapper) {
        return throwable instanceof RuntimeException ? RuntimeException.class.cast(throwable) : runtimeExceptionWrapper.apply(throwable);
    }
}
