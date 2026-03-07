package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NativeConsumerSupplier<K, V> implements ConfigurableConsumerSupplier<K, V> {

    private Map<String, Object> properties = Collections.emptyMap();

    @Override
    public void configure(Map<String, ?> properties) {
        this.properties = new HashMap<>(properties);
        this.properties.remove(CONFIG);
    }

    @Override
    public Consumer<K, V> getConsumer() {
        return new KafkaConsumer<>(properties);
    }
}
