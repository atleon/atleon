package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ConfigurableConsumerSupplier} that returns instances of
 * {@link KafkaConsumer}.
 */
public class NativeConsumerSupplier<K, V> implements ConfigurableConsumerSupplier<K, V> {

    private Map<String, Object> properties = Collections.emptyMap();

    @Override
    public void configure(Map<String, ?> properties) {
        this.properties = properties.entrySet().stream()
                .filter(it -> !it.getKey().equals(CONFIG))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Consumer<K, V> getConsumer() {
        return new KafkaConsumer<>(properties);
    }
}
