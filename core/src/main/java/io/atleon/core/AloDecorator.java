package io.atleon.core;

import io.atleon.util.Configurable;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Interface for implementing the decoration of {@link Alo}s
 *
 * @param <T> The type of data item exposed by decorated {@link Alo}s
 */
@FunctionalInterface
public interface AloDecorator<T> extends Configurable {

    int INNERMOST_ORDER = Integer.MIN_VALUE;

    int OUTERMOST_ORDER = Integer.MAX_VALUE;

    static <T> AloDecorator<T> combine(List<AloDecorator<T>> decorators) {
        return decorators.size() == 1 ? decorators.get(0) : new Composite<>(decorators);
    }

    @Override
    default void configure(Map<String, ?> properties) {}

    default int order() {
        return 0;
    }

    Alo<T> decorate(Alo<T> alo);

    class Composite<T> implements AloDecorator<T> {

        private final List<AloDecorator<T>> decorators;

        private Composite(List<AloDecorator<T>> decorators) {
            this.decorators = decorators.stream()
                    .sorted(Comparator.comparing(AloDecorator::order))
                    .collect(Collectors.toList());
        }

        @Override
        public void configure(Map<String, ?> properties) {
            decorators.forEach(decorator -> decorator.configure(properties));
        }

        @Override
        public int order() {
            return decorators.isEmpty()
                    ? OUTERMOST_ORDER
                    : decorators.get(decorators.size() - 1).order();
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
