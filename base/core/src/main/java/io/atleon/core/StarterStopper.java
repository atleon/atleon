package io.atleon.core;

import reactor.core.publisher.Flux;

/**
 * Interface used to implement dynamic control (i.e. starting and stopping) of resources, like
 * {@link AloStream} instances.
 */
public interface StarterStopper {

    /**
     * Creates a {@link Flux} of {@link Boolean} values that are used to start and stop a resource.
     * Upon emission of {@literal true}, the resource should be ensured to be "started", and on
     * emission of {@literal false}, the resource should be ensured to be "stopped".
     */
    Flux<Boolean> startStop();
}
