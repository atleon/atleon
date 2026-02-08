package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * API through which a provider of {@link Connection} instances can be configured and invoked.
 */
@FunctionalInterface
public interface ConfigurableConnectionSupplier extends Configurable {

    /**
     * When configuring this in properties, the value for this key should either be the qualified
     * name of a {@link ConfigurableConnectionSupplier} implementation or an instance of one.
     */
    String CONFIG = "configurable.connection.supplier";

    static ConfigurableConnectionSupplier load(
            Map<String, ?> properties, Supplier<? extends ConfigurableConnectionSupplier> defaultSupplier) {
        return ConfigLoading.loadConfigured(properties, CONFIG, ConfigurableConnectionSupplier.class, defaultSupplier);
    }

    @Override
    default void configure(Map<String, ?> properties) {}

    Connection getConnection() throws IOException;
}
