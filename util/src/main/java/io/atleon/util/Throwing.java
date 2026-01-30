package io.atleon.util;

import java.io.IOException;
import java.util.function.Function;

public final class Throwing {

    private Throwing() {}

    public static IOException propagateIO(Throwable throwable) {
        return propagate(throwable, IOException.class, IOException::new);
    }

    public static RuntimeException propagate(Throwable throwable) {
        return propagate(throwable, RuntimeException::new);
    }

    public static RuntimeException propagate(
            Throwable throwable, Function<? super Throwable, ? extends RuntimeException> runtimeExceptionWrapper) {
        return propagate(throwable, RuntimeException.class, runtimeExceptionWrapper);
    }

    private static <T extends Throwable> T propagate(
            Throwable throwable, Class<T> desiredType, Function<? super Throwable, ? extends T> wrapper) {
        return desiredType.isInstance(throwable) ? desiredType.cast(throwable) : wrapper.apply(throwable);
    }
}
