package io.atleon.core;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

/**
 * Interface used to resolve properties within the context of configuring stream processes. This
 * interface establishes a convention of using dot-notated property keys and resolving properties
 * (first) as string values. For stream properties, we also establish a convention of first looking
 * up by explicit keys, followed by looking up by default-able keys, and then returning an
 * explicitly passed default value. Stream properties are namespaced according to the
 * implementation of {@link #streamPropertyNamespace()}, which serves as a prefix for initial
 * stream property resolution. Convenience methods are provided for accessing optional and required
 * methods, with providable parsing methods and default values.
 */
public interface StreamPropertyResolver {

    /**
     * Returns the explicitly configured property value for the property key resulting from
     * concatenating this stream's {@link #streamPropertyNamespace() property namespace} and the
     * provided key in dot-notated form. If a value is not found, then an attempt to read a
     * "defaulted" value is made by concatenating {@code stream.defaults} and the provided key,
     * again in dot-notated form. If a value is ultimately found, the provided parser is applied to
     * in order to return a strongly typed value. If a value is not found, then the provided
     * default value will be returned. For example, given a property namespace of
     * {@code stream.magnificent.processing} and a key of {@code concurrency}, the first property
     * looked up will be {@code stream.magnificent.processing.concurrency}, and if that property
     * is not available, next lookup will be {@code stream.defaults.concurrency}, and if that
     * property is again not available, the provided default value will be returned.
     *
     * @param key          The stream-specific property key to query a value for
     * @param parser       A function used to interpret available property as a typed value
     * @param defaultValue Default value to use if property is not available
     * @param <T>          The type of value that will be parsed into
     * @return The explicitly configured or defaulted property value
     */
    default <T> T getStreamProperty(String key, Function<? super String, ? extends T> parser, T defaultValue) {
        return getProperty(streamPropertyNamespace() + "." + key)
            .<T>map(parser)
            .orElseGet(() -> getProperty("stream.defaults." + key, parser, defaultValue));
    }

    /**
     * Convenience method for {@link #getRequiredProperty(String, Function) getRequriedProperty(key, Function.identity())}
     */
    default String getRequiredProperty(String key) {
        return getRequiredProperty(key, Function.identity());
    }

    /**
     * Returns the property value associated with the provided key, parsed using the provided
     * function. If there is no value available for the provided key, a {@link NoSuchElementException}
     * will be thrown.
     */
    default <T> T getRequiredProperty(String key, Function<? super String, ? extends T> parser) {
        return getProperty(key).map(parser).orElseThrow(() -> new NoSuchElementException("Missing property: " + key));
    }

    /**
     * Returns the property value associated with the provided key, parsed using the provided
     * function. If there is no value available for the provided key, the provided default value
     * will be returned.
     */
    default <T> T getProperty(String key, Function<? super String, ? extends T> parser, T defaultValue) {
        return getProperty(key).<T>map(parser).orElse(defaultValue);
    }

    /**
     * Provides an overridable property namespace, which is used as a property key prefix when
     * looking up stream-specific property values.
     */
    String streamPropertyNamespace();

    /**
     * Returns the property value associated with the provided key, if available.
     */
    Optional<String> getProperty(String key);
}
