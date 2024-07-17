package io.atleon.core;

/**
 * Mix-in used to configure dynamic controller (i.e. starting and stopping) of a resource, such as
 * an {@link AloStream}. In the context of an application environment, configurations (such as
 * {@link AloStreamConfig}) may implement this interface in order to activate dynamic starting and
 * stopping of configured resources (such as {@link AloStream}s).
 */
public interface StarterStopperConfig {

    /**
     * Create a new {@link StarterStopper} that will be subscribed to by an application to
     * dynamically start and stop some resource, like an {@link AloStream}.
     */
    StarterStopper buildStarterStopper();
}
