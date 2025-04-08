package io.atleon.core;

/**
 * Interface used to configure and build resources needed to define an {@link AloStream} message
 * processing pipeline
 */
public interface AloStreamConfig {

    /**
     * Overridable convenience method for providing a name for an {@link AloStream}. This name
     * should be unique among all stream configurations in a given application.
     *
     * @return Name used to identify running {@link AloStream} that this config is applied to
     */
    default String name() {
        return AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(getClass());
    }

    /**
     * Returns the maximum amount of concurrency with which to process. Defaults to {@code 1}. Note
     * that simply overriding this to a higher value does not inherently guarantee higher
     * processing capacity; In order to realize higher processing capacity via concurrency, this
     * configuration <i>must be applied to a stream's definition</i> (i.e. via {@code groupBy} or
     * {@code flatMap} operators). A return value of {@code 0} may indicate that the stream is
     * disabled. A return value of {@code 1} may indicate either that the stream does not support
     * higher-than-single concurrency, or that it is explicitly configured for single concurrency.
     *
     * @return The maximum amount of concurrent to be applied to processing
     */
    default int concurrency() {
        return 1;
    }
}
