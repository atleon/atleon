package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

/**
 * An {@link Sinks.EmitFailureHandler} that is useful when it is reasonable/expected that the only
 * reason for possible emission failure (on a Sink) should be due to termination.
 */
public final class ShouldBeTerminatedEmitFailureHandler implements Sinks.EmitFailureHandler {

    private final Logger logger;

    private final Level level;

    public ShouldBeTerminatedEmitFailureHandler(Logger logger) {
        this(logger, Level.INFO);
    }

    public ShouldBeTerminatedEmitFailureHandler(Logger logger, Level level) {
        this.logger = logger;
        this.level = level;
    }

    @Override
    public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
        logger.atLevel(level).log("Sink emission failed. Should mean termination: {}-{}", signalType, emitResult);
        return false;
    }
}
