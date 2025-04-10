package io.atleon.core;

import java.time.Duration;

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
     * configuration <i>must be applied to a stream's definition</i> (i.e. via {@code groupBy}
     * operator(s)). A returned value of {@code 0} may indicate that the stream is disabled. A
     * returned value of {@code 1} may indicate either that the stream does not support
     * non-single concurrency, or that it is explicitly configured for single concurrency.
     */
    default int concurrency() {
        return 1;
    }

    /**
     * For streams that support micro-batching, returns the max size of any given batch. Defaults
     * to {@code 1}. Note that simply overriding this value to a higher value does not inherently
     * guarantee micro-batching behavior; In order for micro-batching to be applied, this
     * configuration <i>must be applied to a stream's definition</i> (i.e. via {@code bufferTimeout}
     * operator(s)). A returned value of {@code 1} may indicate either that the stream definition
     * has no micro-batching behavior, or that the stream is configured to process singleton
     * batches of elements.
     */
    default int batchSize() {
        return 1;
    }

    /**
     * For streams that support micro-batching, returns the maximum amount of time that a batch
     * should wait to receive elements (after receiving an initial element) before being emitted.
     * Note that this configuration is not relevant/used if micro-batching behavior is not applied
     * by a given stream's definition (i.e. via {@code bufferTimeout} operator(s)). See
     * {@link #batchSize()} for more information.
     */
    default Duration batchDuration() {
        return Duration.ofSeconds(1);
    }
}
