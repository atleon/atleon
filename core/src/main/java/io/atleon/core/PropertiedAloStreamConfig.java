package io.atleon.core;

/**
 * A property-aware {@link AloStreamConfig} through which primitive (or nearly-primitive)
 * configuration values may be accessed. By default, combines the conventional stream naming
 * convention with convenience methods from {@link StreamPropertyResolver} to provide both
 * stream-specific and default-able properties in dot-notated form.
 */
public interface PropertiedAloStreamConfig extends AloStreamConfig, StreamPropertyResolver {

    /**
     * {@inheritDoc}
     * <p>
     * Allows overriding under {@code concurrency} stream property.
     */
    @Override
    default int concurrency() {
        return getStreamProperty("concurrency", Integer::valueOf, AloStreamConfig.super.concurrency());
    }
}
