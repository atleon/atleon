package io.atleon.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base class of Config-producing resources
 *
 * @param <P> The type of this ConfigProducer
 */
public abstract class ConfigProvider<T, P extends ConfigProvider<T, P>> {

    private Map<String, Object> properties = Collections.emptyMap();

    public final T create() {
        return create(properties);
    }

    public P with(String key, Object value) {
        Map<String, Object> copiedProperties = new HashMap<>(this.properties);
        copiedProperties.put(key, value);
        P copy = initializeProviderCopy();
        copy.setProperties(copiedProperties);
        return copy;
    }

    public P withAll(Map<String, ?> properties) {
        Map<String, Object> copiedProperties = new HashMap<>(this.properties);
        copiedProperties.putAll(properties);
        P copy = initializeProviderCopy();
        copy.setProperties(copiedProperties);
        return copy;
    }

    protected abstract T create(Map<String, Object> properties);

    protected abstract P initializeProviderCopy();

    protected void setProperties(Map<String, Object> properties) {
        this.properties = properties;
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
