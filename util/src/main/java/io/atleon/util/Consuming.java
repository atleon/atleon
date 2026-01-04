package io.atleon.util;

import java.util.function.Consumer;

public final class Consuming {

    private Consuming() {}

    public static <T> Consumer<T> noOp() {
        return __ -> {};
    }
}
