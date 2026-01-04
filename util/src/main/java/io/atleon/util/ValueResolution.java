package io.atleon.util;

import java.lang.reflect.Field;

public final class ValueResolution {

    private ValueResolution() {}

    public static Object getFieldValue(Object target, Field field) {
        try {
            ensureFieldAccessibility(field);
            return field.get(target);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Failed to get Field Value: field=%s target=%s e=%s", field, target, e));
        }
    }

    private static void ensureFieldAccessibility(Field field) {
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
    }
}
