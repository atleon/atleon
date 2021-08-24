package io.atleon.kafka;

import io.atleon.core.AloFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;

public interface AloConsumerRecordFactory<K, V> extends AloFactory<ConsumerRecord<K, V>>, Configurable {

}
