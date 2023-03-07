package io.atleon.kafka;

import io.atleon.core.AloDecorator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface through which decoration of {@link io.atleon.core.Alo}s referencing
 * {@link ConsumerRecord}s can be implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.kafka.AloKafkaConsumerRecordDecorator} in your project's
 * resource directory.
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public interface AloKafkaConsumerRecordDecorator<K, V> extends AloDecorator<ConsumerRecord<K, V>> {

}
