package io.atleon.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FieldResolution {

    private FieldResolution() {

    }

    public static Map<String, Field> getAllFieldsByName(Class<?> clazz) {
        return getAllFields(clazz).stream().collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    public static Collection<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                fields.add(field);
            }
            clazz = clazz.getSuperclass();
        }
        return fields;
    }
}
