package io.atleon.kafka;

import io.atleon.util.Configurable;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaSendInterceptor<K, V> extends Configurable {

    ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord);
}
