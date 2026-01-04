package io.atleon.aws.sqs;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;

/**
 * An interface for creating a "nacknowledger" ({@link Consumer} of Throwable) that is executed
 * if/when processing of the associated {@link SqsMessage} is exceptionally terminated.
 *
 * @param <T> The deserialized type of received Message bodies
 */
public interface NacknowledgerFactory<T> extends Configurable {

    /**
     * When using {@link VisibilityReset} this configures the number of seconds that a
     * nacknowledged message has its visibility reset by.
     */
    String VISIBILITY_RESET_SECONDS_CONFIG = "sqs.nacknowledger.visibility.reset.seconds";

    @Override
    default void configure(Map<String, ?> properties) {}

    Consumer<Throwable> create(
            ReceivedSqsMessage<T> message,
            SqsMessageVisibilityChanger visibilityChanger,
            Consumer<Throwable> errorEmitter);

    final class Emit<T> implements NacknowledgerFactory<T> {

        Emit() {}

        @Override
        public Consumer<Throwable> create(
                ReceivedSqsMessage<T> message,
                SqsMessageVisibilityChanger visibilityChanger,
                Consumer<Throwable> errorEmitter) {
            return errorEmitter;
        }
    }

    final class VisibilityReset<T> implements NacknowledgerFactory<T> {

        private final Logger logger;

        private int seconds = 0;

        VisibilityReset(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void configure(Map<String, ?> properties) {
            this.seconds = ConfigLoading.loadInt(properties, VISIBILITY_RESET_SECONDS_CONFIG)
                    .orElse(seconds);
        }

        @Override
        public Consumer<Throwable> create(
                ReceivedSqsMessage<T> message,
                SqsMessageVisibilityChanger visibilityChanger,
                Consumer<Throwable> errorEmitter) {
            String messageId = message.messageId(); // Avoid keeping the whole message in memory
            return error -> {
                logger.warn(
                        "Nacknowledging Message id={} by resetting visibility to {} seconds",
                        messageId,
                        seconds,
                        error);
                visibilityChanger.execute(Duration.ofSeconds(seconds), false);
            };
        }
    }
}
