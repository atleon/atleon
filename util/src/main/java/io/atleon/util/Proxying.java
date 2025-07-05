package io.atleon.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public final class Proxying {

    public static <T, U extends T> U interfaceMethods(Class<T> interfaceType, MethodInvocationHandler handler) {
        InvocationHandler rawHandler = (proxy, method, args) -> {
            try {
                return handler.invoke(method, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (U) Proxy.newProxyInstance(interfaceType.getClassLoader(), new Class[]{interfaceType}, rawHandler);
    }

    public interface MethodInvocationHandler {

        Object invoke(Method method, Object[] args) throws ReflectiveOperationException;
    }
}
