package io.atleon.kafka;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Map;
import java.util.function.Supplier;

/**
 * API through which a provider of {@link Consumer} instances can be configured and invoked.
 */
public interface ConfigurableConsumerSupplier<K, V> extends Configurable {

    /**
     * When configuring a consumer supplier in properties, the value for this key can be either the
     * qualified name of a {@link ConfigurableConsumerSupplier} implementation or an instance of
     * one.
     */
    String CONFIG = "configurable.consumer.supplier";

    static <K, V> ConfigurableConsumerSupplier<K, V> load(
            Map<String, ?> properties, Supplier<? extends ConfigurableConsumerSupplier<K, V>> defaultSupplier) {
        return ConfigLoading.loadConfigured(properties, CONFIG, ConfigurableConsumerSupplier.class, defaultSupplier);
    }

    static <K, V> ConfigurableConsumerSupplier<K, V> wrap(Supplier<Consumer<K, V>> supplier) {
        return new ConfigurableConsumerSupplier<K, V>() {
            @Override
            public void configure(Map<String, ?> properties) {}

            @Override
            public Consumer<K, V> getConsumer() {
                return supplier.get();
            }
        };
    }

    Consumer<K, V> getConsumer();
}
