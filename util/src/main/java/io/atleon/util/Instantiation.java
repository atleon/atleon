package io.atleon.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public final class Instantiation {

    private Instantiation() {}

    public static <T> T oneTyped(
            Class<? extends T> typeOrSubType, String qualifiedNameOfSubType, Object... parameters) {
        return oneTyped(typeOrSubType, TypeResolution.classForQualifiedName(qualifiedNameOfSubType), parameters);
    }

    public static <T> T oneTyped(Class<? extends T> typeOrSubType, Class<?> subType, Object... parameters) {
        if (typeOrSubType.isAssignableFrom(subType)) {
            return typeOrSubType.cast(one(subType, parameters));
        } else {
            throw new IllegalArgumentException(
                    "Cannot instantiate type=" + typeOrSubType + " using subType=" + subType);
        }
    }

    public static <T> T one(Class<? extends T> clazz, Object... parameters) {
        try {
            Class<?>[] parameterTypes = Arrays.stream(parameters)
                    .map(Instantiation::deduceParameterClass)
                    .toArray(Class[]::new);
            Constructor<? extends T> constructor = clazz.getDeclaredConstructor(parameterTypes);
            ensureConstructorAccessibility(constructor);
            return constructor.newInstance(parameters);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not instantiate instance of Class: " + clazz, e);
        }
    }

    private static Class<?> deduceParameterClass(Object parameter) {
        if (parameter instanceof Collection) {
            return Collection.class;
        } else {
            return parameter instanceof Map ? Map.class : parameter.getClass();
        }
    }

    private static void ensureConstructorAccessibility(Constructor<?> constructor) {
        if (!Modifier.isPublic(constructor.getModifiers()) && !constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
    }
}
