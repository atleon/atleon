package io.atleon.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class Instantiation {

    private Instantiation() {

    }

    public static <T> T one(String qualifiedName, Object... parameters) {
        return one(TypeResolution.classForQualifiedName(qualifiedName), parameters);
    }

    public static <T> T one(Class<? extends T> clazz, Object... parameters) {
        try {
            List<Class<?>> parameterTypes = Arrays.stream(parameters).map(Instantiation::deduceParameterClass).collect(Collectors.toList());
            Constructor<? extends T> constructor = clazz.getDeclaredConstructor(parameterTypes.toArray(new Class[parameterTypes.size()]));
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

    private static void ensureConstructorAccessibility(Constructor constructor) {
        if (!Modifier.isPublic(constructor.getModifiers()) && !constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
    }
}
