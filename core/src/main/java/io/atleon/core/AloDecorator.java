package io.atleon.core;

import io.atleon.util.Configurable;

import java.util.List;
import java.util.Map;

/**
 * Interface for implementing the decoration of {@link Alo}s
 *
 * @param <T> The type of data item exposed by decorated {@link Alo}s
 */
@FunctionalInterface
public interface AloDecorator<T> extends Configurable {

    static <T> AloDecorator<T> combine(List<AloDecorator<T>> decorators) {
        return decorators.size() == 1 ? decorators.get(0) : new Composite<>(decorators);
    }

    @Override
    default void configure(Map<String, ?> properties) {

    }

    Alo<T> decorate(Alo<T> alo);

    class Composite<T> implements AloDecorator<T> {

        private final List<AloDecorator<T>> decorators;

        private Composite(List<AloDecorator<T>> decorators) {
            this.decorators = decorators;
        }

        @Override
        public Alo<T> decorate(Alo<T> alo) {
            for (AloDecorator<T> decorator : decorators) {
                alo = decorator.decorate(alo);
            }
            return alo;
        }
    }
}
