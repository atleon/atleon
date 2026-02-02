package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Special extension of {@link ConfiguratorConnectionSupplier} that uses
 * {@link AloConnectionFactory} to create {@link ConnectionFactory} instances.
 */
public final class AloConfiguratorConnectionSupplier extends ConfiguratorConnectionSupplier {

    @Override
    protected ConnectionFactory createConnectionFactory(Map<String, ?> properties) {
        Map<String, Object> sanitizedProperties = new HashMap<>(properties);
        consumeDefaultProperties(sanitizedProperties::putIfAbsent);
        return AloConnectionFactory.create(sanitizedProperties);
    }
}
