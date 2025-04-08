package io.atleon.spring;

import io.atleon.core.SelfConfigurableAloStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

/**
 * Spring-aware extension of {@link SelfConfigurableAloStream} that provides some opinionated
 * convenience methods for accessing stream-oriented configuration properties. This class
 * establishes a default convention of locating stream-specific properties under a dot-notated
 * prefix derived from the name of this stream. For example, given default method usage, a stream
 * named {@code MagnificentProcessingStream} will have its properties available under the prefix
 * {@code stream.magnificent.processing}. Certain empirically common configurations are accessible
 * as provided convenience methods. For example, if {@code MagnificentProcessingStream} utilizes
 * non-singleton concurrency, that concurrency can be configured as
 * {@code stream.magnificent.processing.concurrency} and accessed via {@link #concurrency()}.
 */
public abstract class SpringAloStream extends SelfConfigurableAloStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringAloStream.class);

    private final Environment environment;

    protected SpringAloStream(ApplicationContext context) {
        this.environment = context.getEnvironment();
        LOGGER.info("Stream properties for {} configurable under '{}'", name(), streamPropertyNamespace());
    }

    /**
     * For streams that support non-singleton concurrency, returns the maximum amount of
     * concurrency that should be used. Defaults to {@code 1}, therefore in order to realize a
     * non-singleton concurrency value, it must either be explicitly configured for this stream,
     * or a default value must be configured (under {@code stream.defaults.concurrency}).
     */
    protected final int concurrency() {
        return getStreamProperty("concurrency", Integer::valueOf, 1);
    }

    /**
     * For streams that support micro-batching, returns the max size of any given batch. Defaults
     * to {@code 1}, therefore in order to realize non-singleton batch sizes, it must either be
     * explicitly configured for this stream, or a default value must be configured (under
     * {@code stream.defaults.batch.size}).
     */
    protected final int batchSize() {
        return getStreamProperty("batch.size", Integer::valueOf, 1);
    }

    /**
     * For streams that support micro-batching, returns the maximum amount of time that a batch
     * should wait to receive elements before being emitted. Configurable in properties as an
     * ISO-8601 duration, e.g. "PT1M1.5S" == 1 minute + 1 second + 500 milliseconds. Defaults to
     * one second.
     */
    protected final Duration batchDuration() {
        return getStreamProperty("batch.duration", Duration::parse, Duration.ofSeconds(1));
    }

    /**
     * Returns the explicitly configured property value for the property key resulting from
     * concatenating this stream's {@link #streamPropertyNamespace() property namespace} and the
     * provided key in dot-notated form. If this property is not found, then an attempt to read a
     * "defaulted" value is made by concatenating {@code stream.defaults} with the provided key,
     * again in dot-notated form. If a value is ultimately found, the provided parser is applied to
     * in order to return a strongly typed value. If a value is not found, then the provided
     * default value will be returned.
     *
     * @param key          The stream-specific property key to query a value for
     * @param parser       A function used to interpret available property as a typed value
     * @param defaultValue Default value to use if property is not available
     * @param <T>          The type of value that will be parsed into
     * @return The explicitly configured or defaulted property value
     */
    protected final <T> T getStreamProperty(String key, Function<? super String, ? extends T> parser, T defaultValue) {
        return getProperty(streamPropertyNamespace() + "." + key)
            .<T>map(parser)
            .orElseGet(() -> getProperty("stream.defaults." + key, parser, defaultValue));
    }

    /**
     * Provides an overridable property namespace, which is used as a property key prefix when
     * looking up stream-specific property values. By default, this method takes the name of the
     * stream (which is conventionally kebab-cased), removes any existing "-stream" suffix,
     * converts it to dot-notation, and appends the result to "stream.". So, for example, a stream
     * named {@code MagnificentProcessingStream} would have a property namespace of
     * {@code stream.magnificent.processing}.
     */
    protected String streamPropertyNamespace() {
        return "stream." + name().replaceAll("-stream(-\\d+)?$", "").replace('-', '.');
    }

    /**
     * Convenience method for {@link #getRequiredProperty(String) getRequriedProperty(key, Function.identity())}
     */
    protected final String getRequiredProperty(String key) {
        return getRequiredProperty(key, Function.identity());
    }

    /**
     * Returns the property value associated with the provided key, parsed using the provided
     * function. If there is no value available for the provided key, a {@link NoSuchElementException}
     * will be thrown.
     */
    protected final <T> T getRequiredProperty(String key, Function<? super String, ? extends T> parser) {
        return getProperty(key).map(parser).orElseThrow(() -> new NoSuchElementException("Missing property: " + key));
    }

    /**
     * Returns the property value associated with the provided key, parsed using the provided
     * function. If there is no value available for the provided key, the provided default value
     * will be returned.
     */
    protected final <T> T getProperty(String key, Function<? super String, ? extends T> parser, T defaultValue) {
        return getProperty(key).<T>map(parser).orElse(defaultValue);
    }

    /**
     * Returns the available property value associated with the provided key, else empty.
     */
    protected final Optional<String> getProperty(String key) {
        return Optional.ofNullable(environment.getProperty(key));
    }
}
