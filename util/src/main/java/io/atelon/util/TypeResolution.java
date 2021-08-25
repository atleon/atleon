package io.atelon.util;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class TypeResolution {

    private TypeResolution() {

    }

    public static String extractSimpleName(Object object) {
        return safelyGetClass(object).getSimpleName();
    }

    public static Class safelyGetClass(Object object) {
        return object == null ? Void.class : object.getClass();
    }

    public static <T> Class<T> classForPackageAndClass(String pkg, String name) {
        return classForQualifiedName(pkg + "." + name);
    }

    public static <T> Class<T> classForQualifiedName(String qualifiedName) {
        try {
            return (Class<T>) Class.forName(qualifiedName);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not get/find Class with qualified name: " + qualifiedName, e);
        }
    }

    public static boolean isGenericClass(Class clazz) {
        return clazz.getTypeParameters().length > 0;
    }

    public static boolean isDataStructure(Class clazz) {
        return Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz);
    }

    public static Set<Type> getAllTypeParameters(Class clazz) {
        return withSuperClasses(clazz).stream()
            .<TypeVariable[]>map(Class::getTypeParameters)
            .flatMap(Arrays::stream)
            .collect(Collectors.toSet());
    }

    private static List<Class> withSuperClasses(Class clazz) {
        List<Class> classes = new ArrayList<>();
        classes.add(clazz);
        while ((clazz = clazz.getSuperclass()) != null) {
            classes.add(clazz);
        }
        return classes;
    }
}
