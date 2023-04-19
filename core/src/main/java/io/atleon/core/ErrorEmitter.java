package io.atleon.core;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

/**
 * Utility class wrapping a {@link Sinks.Empty} that allows applying to Publishers and
 * safe, timeout-based error emission.
 *
 * @param <T> The type of elements emitted in merged Publishers
 */
public final class ErrorEmitter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorEmitter.class);

    private final Duration timeout;

    private final Sinks.Empty<T> sink = Sinks.empty();

    private ErrorEmitter(Duration timeout) {
        this.timeout = timeout;
    }

    public static <T> ErrorEmitter<T> create(Duration timeout) {
        return new ErrorEmitter<>(timeout);
    }

    public Flux<T> applyTo(Publisher<T> publisher) {
        return Flux.from(publisher).mergeWith(sink.asMono());
    }

    public void safelyEmit(Throwable error) {
        try {
            sink.emitError(error, Sinks.EmitFailureHandler.busyLooping(timeout));
        } catch (Sinks.EmissionException emissionException) {
            LOGGER.info("Failed to emit error due to reason=" + emissionException.getReason());
        }
    }
}
