package io.atleon.core;

import java.util.Optional;

/**
 * Base interface representing the result of sending a message
 */
public interface SenderResult {

    Optional<Throwable> failureCause();
}
