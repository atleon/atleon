package io.atleon.core;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Interface used to read properties within the context of configuring stream processes. This
 * interface establishes a convention of using dot-notated property keys, and for stream
 * properties, lookup is first done by explicit keys, followed by lookup by default-able keys,
 * and then returning any explicitly passed fallback value. Stream properties are namespaced
 * according to the implementation of {@link #streamPropertyNamespace()}, which serves as a prefix
 * for initial stream property resolution. Default properties are namespaced according to the
 * implementation of {@link #defaultablePropertyNamespace()}, which serves as a prefix for
 * secondary stream property resolution. Convenience methods are provided for accessing optional
 * and required methods, with providable parsing types and fallback values.
 */
public interface StreamPropertyResolver {

    /**
     * Convenience method for {@link #getStreamProperty(String, Class) getStreamProperty(key, type).orElse(fallbackValue)}
     */
    default <T> T getStreamProperty(String key, Class<T> type, T fallbackValue) {
        return getStreamProperty(key, type).orElse(fallbackValue);
    }

    /**
     * Returns the explicitly configured property value for the property key resulting from
     * concatenating this stream's {@link #streamPropertyNamespace() property namespace} and the
     * provided key in dot-notated form (by default implementation). If a value is not found, then
     * an attempt to read a "defaulted" value is made by concatenating this stream's
     * {@link #defaultablePropertyNamespace() default property namespace} and the provided key,
     * again in dot-notated form (by default implementation). If a value is ultimately found, the
     * provided type is applied to return a strongly typed value. If a value is not found, then an
     * empty result will be returned. For example, given a property namespace of
     * {@code stream.magnificent.processing} and a key of {@code concurrency}, the first property
     * lookup will be {@code stream.magnificent.processing.concurrency}, and if that property is
     * not available, the next lookup will be {@code stream.defaults.concurrency}, and if that
     * property is also not available, an empty result will be returned..
     *
     * @param key  The stream-specific property key to query a value for
     * @param type The type to parse available property value as
     * @param <T>  The type of value that will be parsed into
     * @return The parsed property value for the given key
     */
    default <T> Optional<T> getStreamProperty(String key, Class<T> type) {
        Optional<T> value = getProperty(streamPropertyNamespace() + "." + key, type);
        return value.isPresent() ? value : getProperty(defaultablePropertyNamespace() + "." + key, type);
    }

    /**
     * Convenience method for {@link #getRequiredProperty(String, Class) getRequriedProperty(key, String.class)}
     */
    default String getRequiredProperty(String key) {
        return getRequiredProperty(key, String.class);
    }

    /**
     * Returns the property value associated with the provided key, parsed as the provided type. If
     * there is no value available for the provided key, a {@link NoSuchElementException} will be
     * thrown.
     */
    default <T> T getRequiredProperty(String key, Class<T> type) {
        return getProperty(key, type).orElseThrow(() -> new NoSuchElementException("Missing property: " + key));
    }

    /**
     * Convenience method for {@link #getProperty(String, Class) getProperty(key, type).orElse(defaultValue)}
     */
    default <T> T getProperty(String key, Class<T> type, T defaultValue) {
        return getProperty(key, type).orElse(defaultValue);
    }

    /**
     * Provides an overridable property namespace, which is used as a property key prefix when
     * looking up stream-specific property values.
     */
    String streamPropertyNamespace();

    /**
     * Namespace under which default stream properties may be located.
     */
    String defaultablePropertyNamespace();

    /**
     * Returns the property value associated with the provided key, parsed as the provided type,
     * if available.
     */
    <T> Optional<T> getProperty(String key, Class<T> type);
}
