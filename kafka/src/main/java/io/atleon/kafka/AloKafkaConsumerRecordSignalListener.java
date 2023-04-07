package io.atleon.kafka;

import io.atleon.core.AloSignalListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface through which side effects on {@link reactor.core.publisher.Signal}s emitted from
 * Reactor Publishers of {@link io.atleon.core.Alo}s referencing {@link ConsumerRecord}s can be
 * implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.aws.sqs.AloKafkaConsumerRecordSignalListener} in your
 * project's resources directory.
 *
 * @param <K> The types of keys in records consumed by this listener
 * @param <V> The types of values in records consumed by this listener
 */
public interface AloKafkaConsumerRecordSignalListener<K, V> extends AloSignalListener<ConsumerRecord<K, V>> {

}
