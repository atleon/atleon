package io.atleon.util;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProxyingTest {

    @Test
    public void interfaceMethods_givenNonInterface_expectsIllegalArgumentException() {
        assertThrows(
            IllegalArgumentException.class,
            () -> Proxying.interfaceMethods(ProxyingTest.class, (method, args) -> null));
    }

    @Test
    public void interfaceMethods_givenDelegateMethod_expectsProxiedDelegation() {
        AtomicReference<Object[]> invocationArgs = new AtomicReference<>();

        Configurable configurable = Proxying.interfaceMethods(Configurable.class, (method, args) -> {
            invocationArgs.set(args);
            return null;
        });

        Map<String, String> properties = Collections.singletonMap("key", "value");
        configurable.configure(properties);

        assertNotNull(invocationArgs.get());
        assertEquals(1, invocationArgs.get().length);
        assertEquals(properties, invocationArgs.get()[0]);
    }
}