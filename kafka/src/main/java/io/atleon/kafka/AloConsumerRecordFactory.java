package io.atleon.kafka;

import io.atleon.core.AloFactory;
import io.atleon.util.Configurable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface AloConsumerRecordFactory<K, V> extends AloFactory<ConsumerRecord<K, V>>, Configurable {

}
