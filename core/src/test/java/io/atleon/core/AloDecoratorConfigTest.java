package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AloDecoratorConfigTest {

    @Test
    public void decoratorsAreAutoLoadedIfNotSpecifiedDirectly() {
        Optional<AloDecorator<Object>> decorator = AloDecoratorConfig.load(AloDecorator.class, Collections.emptyMap(), "");

        assertTrue(decorator.isPresent());
        assertTrue(decorator.get() instanceof TestAloDecorator);
    }
}