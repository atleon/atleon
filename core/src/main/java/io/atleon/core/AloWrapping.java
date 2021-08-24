package io.atleon.core;

import java.util.function.Function;
import java.util.function.Predicate;

public final class AloWrapping {

    private AloWrapping() {

    }

    public static <T, A extends Alo<T>> Predicate<A> wrapFilter(Predicate<? super A> filter) {
        return alo -> {
            try {
                return filter.test(alo);
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }

    public static <T, A extends Alo<T>, R> Function<A, R> wrapMapper(Function<? super A, ? extends R> mapper) {
        return alo -> {
            try {
                return mapper.apply(alo);
            } catch (Throwable error) {
                Alo.nacknowledge(alo, error);
                throw Throwing.propagate(error);
            }
        };
    }
}
