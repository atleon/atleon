package io.atleon.core;

import java.time.Duration;

/**
 * A property-aware {@link AloStreamConfig} through which primitive (or nearly-primitive)
 * configuration values may be accessed. By default, combines the conventional stream naming
 * convention with convenience methods from {@link StreamPropertyResolver} to provide both
 * stream-specific and default-able properties. For example, given default method usage, a stream
 * named {@code MagnificentProcessingStream} would have a stream property namespace of
 * {code stream.magnificent.processing}, and values for (e.g.) {@code concurrency} would be
 * configurable explicitly as {@code stream.magnificent.processing.concurrency}, or default-able as
 * {@code stream.defaults.concurrency}.
 */
public interface PropertiedAloStreamConfig extends AloStreamConfig, StreamPropertyResolver {

    /**
     * {@inheritDoc}
     *
     * Allows overriding under {@code concurrency} stream property.
     */
    @Override
    default int concurrency() {
        return getStreamProperty("concurrency", Integer::valueOf, AloStreamConfig.super.concurrency());
    }

    /**
     * {@inheritDoc}
     *
     * Allows overriding under {@code batch.size} stream property.
     */
    @Override
    default int batchSize() {
        return getStreamProperty("batch.size", Integer::valueOf, AloStreamConfig.super.batchSize());
    }

    /**
     * {@inheritDoc}
     *
     * Allows overriding under {@code batch.duration} stream property as an ISO-8601 duration. For
     * example, "PT1M1.5S" == 1 minute + 1 second + 500 milliseconds.
     */
    @Override
    default Duration batchDuration() {
        return getStreamProperty("batch.duration", Duration::parse, AloStreamConfig.super.batchDuration());
    }

    /**
     * {@inheritDoc}
     *
     * By default, this method takes the name of the stream (which is conventionally kebab-cased),
     * removes any existing "-stream" suffix, converts it to dot-notation, and appends the result
     * to "stream.". So, for example, a stream named {@code MagnificentProcessingStream} would have
     * a property namespace of {@code stream.magnificent.processing}.
     */
    @Override
    default String streamPropertyNamespace() {
        return "stream." + name().replaceAll("-stream(-\\d+)?$", "").replace('-', '.');
    }
}
