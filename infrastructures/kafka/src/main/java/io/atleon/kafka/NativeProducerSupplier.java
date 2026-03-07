package io.atleon.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NativeProducerSupplier<K, V> implements ConfigurableProducerSupplier<K, V> {

    private Map<String, Object> properties = Collections.emptyMap();

    @Override
    public void configure(Map<String, ?> properties) {
        this.properties = new HashMap<>(properties);
        this.properties.remove(CONFIG);
    }

    @Override
    public Producer<K, V> getProducer() {
        return new KafkaProducer<>(properties);
    }
}
