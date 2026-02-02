package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;
import io.atleon.util.Calling;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ConfigurableConnectionSupplier} that creates {@link Connection}s
 * through {@link ConnectionFactory} instances, which are configured through the
 * {@link ConnectionFactoryConfigurator} API.
 */
public class ConfiguratorConnectionSupplier implements ConfigurableConnectionSupplier {

    private ConnectionFactory connectionFactory;

    @Override
    public void configure(Map<String, ?> properties) {
        this.connectionFactory = createConnectionFactory(properties);
    }

    @Override
    public final Connection getConnection() throws IOException {
        return Calling.callAsIO(connectionFactory::newConnection);
    }

    protected ConnectionFactory createConnectionFactory(Map<String, ?> properties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        ConnectionFactoryConfigurator.load(connectionFactory, sanitizeProperties(properties), "");
        return connectionFactory;
    }

    protected static Map<String, String> sanitizeProperties(Map<String, ?> properties) {
        Map<String, String> sanitizedProperties = properties.entrySet().stream()
                .filter(it -> !it.getKey().equals(ConfigurableConnectionSupplier.CONFIG))
                .collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().toString()));
        consumeDefaultProperties(sanitizedProperties::putIfAbsent);
        return sanitizedProperties;
    }

    protected static void consumeDefaultProperties(BiConsumer<String, ? super String> consumer) {
        consumer.accept(ConnectionFactoryConfigurator.USE_NIO, "true");
    }
}
