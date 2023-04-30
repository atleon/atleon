package io.atleon.core;

/**
 * Interface used to configure and build resources needed to define an {@link AloStream} message
 * processing pipeline
 */
public interface AloStreamConfig {

    /**
     * Overridable convenience method for providing a name for an {@link AloStream}
     *
     * @return Name used to identify running {@link AloStream} that this config is applied to
     */
    default String name() {
        return AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(getClass());
    }
}
