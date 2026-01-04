package io.atleon.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.jspecify.annotations.Nullable;

/**
 * Utility methods for creating/managing proxies.
 */
public final class Proxying {

    public static <T, U extends T> U interfaceMethods(Class<T> interfaceType, MethodInvocationHandler handler) {
        if (!interfaceType.isInterface()) {
            throw new IllegalArgumentException("Type to proxy must be an interface");
        }
        InvocationHandler rawHandler = (proxy, method, args) -> {
            try {
                return handler.invoke(method, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (U) Proxy.newProxyInstance(interfaceType.getClassLoader(), new Class[] {interfaceType}, rawHandler);
    }

    public interface MethodInvocationHandler {

        @Nullable
        Object invoke(Method method, Object[] args) throws ReflectiveOperationException;
    }
}
