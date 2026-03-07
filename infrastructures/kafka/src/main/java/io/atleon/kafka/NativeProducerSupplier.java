package io.atleon.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ConfigurableProducerSupplier} that returns instances of
 * {@link KafkaProducer}.
 */
public class NativeProducerSupplier<K, V> implements ConfigurableProducerSupplier<K, V> {

    private Map<String, Object> properties = Collections.emptyMap();

    @Override
    public void configure(Map<String, ?> properties) {
        this.properties = properties.entrySet().stream()
                .filter(it -> !it.getKey().equals(CONFIG))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Producer<K, V> getProducer() {
        return new KafkaProducer<>(properties);
    }
}
