package io.atleon.kafka;

import io.atleon.context.ContextActivatingAloDecorator;
import io.atleon.core.Alo;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An {@link AloKafkaConsumerRecordDecorator} that decorates {@link Alo} elements with
 * {@link io.atleon.context.AloContext AloContext} activation
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public class ContextActivatingAloKafkaConsumerRecordDecorator<K, V>
    extends ContextActivatingAloDecorator<ConsumerRecord<K, V>>
    implements AloKafkaConsumerRecordDecorator<K, V> {

}
