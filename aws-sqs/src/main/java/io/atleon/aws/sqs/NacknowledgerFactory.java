package io.atleon.aws.sqs;

import io.atleon.util.Configurable;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An interface for creating a "nacknowledger" ({@link Consumer} of Throwable) that is executed
 * if/when processing of the associated {@link SqsMessage} is exceptionally terminated.
 *
 * @param <T> The deserialized type of received Message bodies
 */
public interface NacknowledgerFactory<T> extends Configurable {

    Consumer<Throwable> create(
        ReceivedSqsMessage<T> message,
        Runnable deleter,
        SqsMessageVisibilityChanger visibilityChanger,
        Consumer<Throwable> errorEmitter
    );

    final class Emit<T> implements NacknowledgerFactory<T> {

        Emit() {

        }

        @Override
        public void configure(Map<String, ?> properties) {

        }

        @Override
        public Consumer<Throwable> create(
            ReceivedSqsMessage<T> message,
            Runnable deleter,
            SqsMessageVisibilityChanger visibilityChanger,
            Consumer<Throwable> errorEmitter
        ) {
            return errorEmitter;
        }
    }

    final class VisibilityReset<T> implements NacknowledgerFactory<T> {

        private final Logger logger;

        private final int seconds;

        VisibilityReset(Logger logger, int seconds) {
            this.logger = logger;
            this.seconds = seconds;
        }

        @Override
        public void configure(Map<String, ?> properties) {

        }

        @Override
        public Consumer<Throwable> create(
            ReceivedSqsMessage<T> message,
            Runnable deleter,
            SqsMessageVisibilityChanger visibilityChanger,
            Consumer<Throwable> errorEmitter
        ) {
            String messageId = message.messageId(); // Avoid keeping the whole message in memory
            return error -> {
                logger.warn("Nacknowledging SQS Message with id={} by resetting its visibility to ZERO", messageId, error);
                visibilityChanger.execute(Duration.ofSeconds(seconds), false);
            };
        }
    }
}
