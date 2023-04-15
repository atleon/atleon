package io.atleon.kafka;

import io.atleon.util.Configurable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.function.Consumer;

/**
 * An interface for creating a "nacknowledger" ({@link Consumer} of Throwable) that is executed
 * if/when processing of the associated {@link ConsumerRecord} is exceptionally terminated.
 *
 * @param <K> inbound record key type
 * @param <V> inbound record value type
 */
public interface NacknowledgerFactory<K, V> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {

    }

    Consumer<Throwable> create(ConsumerRecord<K, V> consumerRecord, Consumer<Throwable> errorEmitter);

    final class Emit<K, V> implements NacknowledgerFactory<K, V> {

        Emit() {

        }

        @Override
        public Consumer<Throwable> create(ConsumerRecord<K, V> consumerRecord, Consumer<Throwable> errorEmitter) {
            return errorEmitter;
        }
    }
}
