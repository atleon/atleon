package io.atleon.core;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * A context conventionally used by Config implementations to access dependent beans and properties
 */
public interface ConfigContext {

    /**
     * Return the bean instance that uniquely matches the given object type, if any.
     */
    <T> T getBean(Class<T> clazz);

    /**
     * Return the bean instance that uniquely matches the given name and object type, if any.
     */
    <T> T getBean(String name, Class<T> clazz);

    /**
     * Get all properties that start with the provided prefix, with the prefix removed.
     */
    Map<String, String> getPropertiesPrefixedBy(String prefix);

    /**
     * Return the property value associated with the given key, or {@code defaultValue} if the key
     * cannot be resolved.
     */
    default <T> T getProperty(String key, Class<T> clazz, T defaultValue) {
        return findProperty(key, clazz).orElse(defaultValue);
    }

    /**
     * Return the property value associated with the given key (never {@code null}).
     */
    default String getProperty(String key) {
        return findProperty(key).orElseThrow(() -> new NoSuchElementException("No property for key: " + key));
    }

    /**
     * Return the property value associated with the given key, parsed as the provided type (never
     * {@code null}).
     */
    default <T> T getProperty(String key, Class<T> clazz) {
        return findProperty(key, clazz).orElseThrow(() -> new NoSuchElementException("No property for key: " + key));
    }

    /**
     * Return the property value associated with the given key, or {@link Optional#empty()} if not
     * available.
     */
    Optional<String> findProperty(String key);

    /**
     * Return the property value associated with the given key, parsed as the provided type, or
     * {@link Optional#empty()} if not available.
     */
    <T> Optional<T> findProperty(String key, Class<T> clazz);
}
