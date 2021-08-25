package io.atleon.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Base class of Config-producing resources
 *
 * @param <P> The type of this ConfigProducer
 */
public abstract class ConfigProducer<P extends ConfigProducer<P>> {

    final Map<String, Object> properties = new HashMap<>();

    public P with(String key, Object value) {
        put(key, value);
        return (P) this;
    }

    public Object put(String key, Object value) {
        return properties.put(key, value);
    }

    protected P copyInto(Supplier<P> copySupplier) {
        P copy = copySupplier.get();
        properties.forEach(copy::put);
        return copy;
    }

    protected static void validateNonNullProperty(Map<String, Object> properties, String key) {
        Objects.requireNonNull(properties.get(key), key + " is a required Configuration");
    }

    protected static <T extends Enum<T>> void validateEnumProperty(Map<String, Object> properties, String key, Class<T> enumClass) {
        try {
            Enum.valueOf(enumClass, Objects.toString(properties.get(key)));
        } catch (Exception e) {
            throw new IllegalArgumentException(key + " must be configured as an Enum value from " + enumClass, e);
        }
    }
}
