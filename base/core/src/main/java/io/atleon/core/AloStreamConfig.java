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
     * Overridable configuration for determining whether {@link AloStream}s configured by this
     * config should have automated starting (and stopping) behavior enabled. This is useful in the
     * context of application development where stream lifecycle is tied to the application
     * lifecycle. Defaults to {@code true}. Note that disabling autostart takes precedence over any
     * other configured start (or stop) automation, including usage of {@link StarterStopper}.
     *
     * @return Whether automated starting (and/or stopping) of configured stream is enabled
     */
    default Autostart autostart() {
        return Autostart.ENABLED;
    }
}
