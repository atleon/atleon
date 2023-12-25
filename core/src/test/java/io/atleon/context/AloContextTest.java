package io.atleon.context;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloContextTest {

    private static final AloContext.Key<Long> SINGLE_KEY = AloContext.Key.single("single");

    private static final AloContext.Key<Long> MIN_KEY = AloContext.Key.min("min");

    @Test
    public void keyCannotBeChangedOnceSet() {
        AloContext context = AloContext.create();

        assertTrue(context.set(SINGLE_KEY, 1L));
        assertFalse(context.set(SINGLE_KEY, 2L));
        assertEquals(1L, context.get(SINGLE_KEY).orElse(null));
    }

    @Test
    public void activeContextIsANoOpWhenNoneIsSet() {
        AloContext context = AloContext.active();

        context.set(SINGLE_KEY, 1L);

        assertFalse(context.get(SINGLE_KEY).isPresent());
    }

    @Test
    public void activeContextIsAvailableWhenSet() {
        AloContext context = AloContext.create();

        context.set(SINGLE_KEY, 1L);

        context.run(() -> assertEquals(1L, AloContext.active().get(SINGLE_KEY).orElse(null)));
        context.supply(() -> {
            assertEquals(1L, AloContext.active().get(SINGLE_KEY).orElse(null));
            return new Object();
        });
    }

    @Test
    public void contextsAreCorrectlyActivatedAndRestoredWhenEmbedded() {
        AloContext context1 = AloContext.create();
        AloContext context2 = AloContext.create();

        context1.set(SINGLE_KEY, 1L);
        context2.set(SINGLE_KEY, 2L);

        context1.run(() -> {
            assertEquals(1L, AloContext.active().get(SINGLE_KEY).orElse(null));
            context2.run(() -> assertEquals(2, AloContext.active().get(SINGLE_KEY).orElse(null)));
            assertEquals(1L, AloContext.active().get(SINGLE_KEY).orElse(null));
        });
    }

    @Test
    public void contextsAreMergedBasedOnReduction() {
        AloContext context1 = AloContext.create();
        AloContext context2 = AloContext.create();
        AloContext context3 = AloContext.create();

        context1.set(SINGLE_KEY, 1L);
        context1.set(MIN_KEY, 1L);

        context2.set(SINGLE_KEY, 2L);
        context2.set(MIN_KEY, 2L);

        context3.set(SINGLE_KEY, 3L);
        context3.set(MIN_KEY, 3L);

        AloContext merged = AloContext.create();
        merged.merge(context2);
        merged.merge(context1);
        merged.merge(context3);

        merged.run(() -> {
            assertEquals(2L, AloContext.active().get(SINGLE_KEY).orElse(null));
            assertEquals(1L, AloContext.active().get(MIN_KEY).orElse(null));
        });
    }
}