package io.atleon.core;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Interface used to read properties within the context of configuring a stream process. This
 * interface establishes a convention of using dot-notated property keys, and for stream
 * properties, lookup is first done by stream-specific keys, followed by lookup by default-able
 * keys. Stream properties are namespaced according to the implementation of
 * {@link #specificStreamPropertyNamespace()}, which serves as a prefix
 * for primary stream property resolution. Default properties are namespaced according to the
 * implementation of {@link #defaultStreamPropertyNamespace()}, which serves as a prefix for
 * secondary stream property resolution. Convenience methods are provided for accessing required
 * properties.
 */
public interface StreamPropertyResolver {

    /**
     * Returns the stream-specific property value for the property key resulting from concatenating
     * this stream's {@link #specificStreamPropertyNamespace() property namespace} and the provided
     * key in dot-notated form (by default implementation). If a value is not found, then an
     * attempt to read a "defaulted" value is made by concatenating this stream's
     * {@link #defaultStreamPropertyNamespace() default property namespace} and the provided key,
     * again in dot-notated form (by default implementation). If a value is ultimately found, the
     * provided Class is applied to return a strongly typed value. If a value is not found, then an
     * empty result will be returned. For example, given a stream-specific property namespace of
     * {@code stream.magnificent.processing}, a default stream property namespace of
     * {@code stream.defaults}, and a key of {@code concurrency}, the first property lookup will be
     * {@code stream.magnificent.processing.concurrency}, and if that property is not available,
     * the next lookup will be {@code stream.defaults.concurrency}, and if that property is also
     * not available, an empty result will be returned.
     *
     * @param key  The stream-specific property key to query a value for
     * @param type The type to parse available property value as
     * @param <T>  The type of value that will be parsed into
     * @return The parsed property value for the given key
     */
    default <T> Optional<T> getStreamProperty(String key, Class<T> type) {
        Optional<T> value = getProperty(specificStreamPropertyNamespace() + "." + key, type);
        return value.isPresent() ? value : getProperty(defaultStreamPropertyNamespace() + "." + key, type);
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
     * Namespace for explicitly defined properties of a stream, which is used as a property key
     * prefix when looking up primary stream property values.
     */
    String specificStreamPropertyNamespace();

    /**
     * Namespace under which default stream properties may be located, which is used as a property
     * key prefix when looking up secondary stream property values.
     */
    String defaultStreamPropertyNamespace();

    /**
     * Returns the property value associated with the provided key, parsed as the provided type,
     * if available.
     */
    <T> Optional<T> getProperty(String key, Class<T> type);
}
