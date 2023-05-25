package io.atleon.core;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Interface used to encapsulate extraction of components used to build {@link Alo} items from
 * native types.
 *
 * @param <T> The native type from which {@link Alo} components are extracted
 * @param <V> The type of payload referenced by resulting {@link Alo} items
 */
public interface AloComponentExtractor<T, V> {

    static <T, V> AloComponentExtractor<T, V> composed(
        Function<T, Runnable> acknowledgerExtractor,
        Function<T, Consumer<? super Throwable>> nacknowledgerExtractor,
        Function<? super T, ? extends V> valueExtractor
    ) {
        return new Composed<>(acknowledgerExtractor, nacknowledgerExtractor, valueExtractor);
    }

    Runnable nativeAcknowledger(T t);

    Consumer<? super Throwable> nativeNacknowledger(T t);

    V value(T t);

    class Composed<T, V> implements AloComponentExtractor<T, V> {

        private final Function<T, Runnable> acknowledgerExtractor;

        private final Function<T, Consumer<? super Throwable>> nacknowledgerExtractor;

        private final Function<? super T, ? extends V> valueExtractor;

        private Composed(
            Function<T, Runnable> acknowledgerExtractor,
            Function<T, Consumer<? super Throwable>> nacknowledgerExtractor,
            Function<? super T, ? extends V> valueExtractor
        ) {
            this.acknowledgerExtractor = acknowledgerExtractor;
            this.nacknowledgerExtractor = nacknowledgerExtractor;
            this.valueExtractor = valueExtractor;
        }

        @Override
        public Runnable nativeAcknowledger(T t) {
            return acknowledgerExtractor.apply(t);
        }

        @Override
        public Consumer<? super Throwable> nativeNacknowledger(T t) {
            return nacknowledgerExtractor.apply(t);
        }

        @Override
        public V value(T t) {
            return valueExtractor.apply(t);
        }
    }
}
