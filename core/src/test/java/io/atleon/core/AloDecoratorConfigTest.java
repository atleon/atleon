package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AloDecoratorConfigTest {

    @Test
    public void decoratorsAreAutoLoadedIfNotSpecifiedDirectly() {
        Optional<AloDecorator<Object>> loadedDecorator =
                AloDecoratorConfig.load(Collections.emptyMap(), AloDecorator.class);

        assertTrue(loadedDecorator.isPresent());
        assertTrue(loadedDecorator.get() instanceof TestAloDecorator);
    }

    @Test
    public void decoratorsArePassedThroughIfExplicitlySpecified() {
        AloDecorator<Object> aloDecorator = new ExplicitAloDecorator<>();

        Map<String, ?> properties = Collections.singletonMap(AloDecoratorConfig.DECORATOR_TYPES_CONFIG, aloDecorator);

        Optional<AloDecorator<Object>> loadedDecorator = AloDecoratorConfig.load(properties, AloDecorator.class);

        assertSame(aloDecorator, loadedDecorator.orElse(null));
    }

    @Test
    public void decoratorsAreInstantiatedFromClassWhenExplicitlySpecified() {
        Map<String, ?> properties =
                Collections.singletonMap(AloDecoratorConfig.DECORATOR_TYPES_CONFIG, ExplicitAloDecorator.class);

        Optional<AloDecorator<Object>> loadedDecorator = AloDecoratorConfig.load(properties, AloDecorator.class);

        assertTrue(loadedDecorator.map(ExplicitAloDecorator.class::isInstance).orElse(false));
    }

    @Test
    public void decoratorsAreInstantiatedFromClassNameWhenExplicitlySpecified() {
        Map<String, ?> properties = Collections.singletonMap(
                AloDecoratorConfig.DECORATOR_TYPES_CONFIG, ExplicitAloDecorator.class.getName());

        Optional<AloDecorator<Object>> loadedDecorator = AloDecoratorConfig.load(properties, AloDecorator.class);

        assertTrue(loadedDecorator.map(ExplicitAloDecorator.class::isInstance).orElse(false));
    }

    @Test
    public void decoratorsAreLoadedWithAutoDecoratorsWhenExplicitlySpecified() {
        Map<String, ?> properties = Collections.singletonMap(
                AloDecoratorConfig.DECORATOR_TYPES_CONFIG,
                Arrays.asList(ExplicitAloDecorator.class, AloDecoratorConfig.DECORATOR_TYPE_AUTO));

        Optional<AloDecorator<Object>> loadedDecorator = AloDecoratorConfig.load(properties, AloDecorator.class);

        assertTrue(loadedDecorator.map(AloDecorator.Composite.class::isInstance).orElse(false));
    }

    public static final class ExplicitAloDecorator<T> implements AloDecorator<T> {

        @Override
        public Alo<T> decorate(Alo<T> alo) {
            return alo;
        }
    }
}
