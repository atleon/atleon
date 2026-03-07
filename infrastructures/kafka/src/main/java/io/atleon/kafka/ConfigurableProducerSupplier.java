package io.atleon.kafka;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;
import java.util.function.Supplier;

/**
 * API through which a provider of {@link Producer} instances can be configured and invoked.
 */
public interface ConfigurableProducerSupplier<K, V> extends Configurable {

    /**
     * When configuring a producer supplier in properties, the value for this key can be either the
     * qualified name of a {@link ConfigurableProducerSupplier} implementation or an instance of
     * one.
     */
    String CONFIG = "configurable.producer.supplier";

    static <K, V> ConfigurableProducerSupplier<K, V> load(
            Map<String, ?> properties, Supplier<? extends ConfigurableProducerSupplier<K, V>> defaultSupplier) {
        return ConfigLoading.loadConfigured(properties, CONFIG, ConfigurableProducerSupplier.class, defaultSupplier);
    }

    static <K, V> ConfigurableProducerSupplier<K, V> wrap(Supplier<Producer<K, V>> supplier) {
        return new ConfigurableProducerSupplier<K, V>() {
            @Override
            public void configure(Map<String, ?> properties) {}

            @Override
            public Producer<K, V> getProducer() {
                return supplier.get();
            }
        };
    }

    Producer<K, V> getProducer();
}
