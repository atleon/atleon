package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AloDecoratorTest {

    @Test
    public void decoratorsAreAppliedInSortedOrder() {
        List<Integer> effects = new ArrayList<>();

        AloDecorator<String> decorator1 = new SideEffectAloDecorator<>(1, effects::add);
        AloDecorator<String> decorator2 = new SideEffectAloDecorator<>(2, effects::add);
        AloDecorator<String> decorator3 = new SideEffectAloDecorator<>(3, effects::add);

        AloDecorator.combine(Arrays.asList(decorator2, decorator3, decorator1))
            .decorate(new TestAlo("data"));

        assertEquals(Arrays.asList(1, 2, 3), effects);
    }

    private static final class SideEffectAloDecorator<T> implements AloDecorator<T> {

        private final int order;

        private final Consumer<Integer> sideEffect;

        public SideEffectAloDecorator(int order, Consumer<Integer> sideEffect) {
            this.order = order;
            this.sideEffect = sideEffect;
        }

        @Override
        public int order() {
            return order;
        }

        @Override
        public Alo<T> decorate(Alo<T> alo) {
            sideEffect.accept(order);
            return alo;
        }
    }
}