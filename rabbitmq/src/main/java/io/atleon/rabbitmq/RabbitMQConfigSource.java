package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.core.ConfigSource;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class RabbitMQConfigSource extends ConfigSource<RabbitMQConfig, RabbitMQConfigSource> {

    public static final String HOST_PROPERTY = "host";

    public static final String PORT_PROPERTY = "port";

    public static final String VIRTUAL_HOST_PROPERTY = "virtual-host";

    public static final String USERNAME_PROPERTY = "username";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String SSL_PROPERTY = "ssl";

    public static final String DISABLED_CONFIG = "disabled";

    protected RabbitMQConfigSource() {

    }

    protected RabbitMQConfigSource(String name) {
        super(name);
    }

    protected RabbitMQConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        super(propertiesToName);
    }

    public static RabbitMQConfigSource named(String name) {
        return new RabbitMQConfigSource(name);
    }

    public RabbitMQConfigSource withHost(String host) {
        return with(HOST_PROPERTY, host);
    }

    public RabbitMQConfigSource withPort(String port) {
        return with(PORT_PROPERTY, port);
    }

    public RabbitMQConfigSource withPort(int port) {
        return with(PORT_PROPERTY, port);
    }

    public RabbitMQConfigSource withVirtualHost(String virtualHost) {
        return with(VIRTUAL_HOST_PROPERTY, virtualHost);
    }

    public RabbitMQConfigSource withUsername(String username) {
        return with(USERNAME_PROPERTY, username);
    }

    public RabbitMQConfigSource withPassword(String password) {
        return with(PASSWORD_PROPERTY, password);
    }

    public RabbitMQConfigSource withSsl(String ssl) {
        return with(SSL_PROPERTY, ssl);
    }

    @Override
    protected RabbitMQConfigSource initializeSourceCopy() {
        return new RabbitMQConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateAddressProperties(properties);
        validateNonNullProperty(properties, VIRTUAL_HOST_PROPERTY);
        validateNonNullProperty(properties, USERNAME_PROPERTY);
        validateNonNullProperty(properties, PASSWORD_PROPERTY);
    }

    @Override
    protected RabbitMQConfig postProcessProperties(Map<String, Object> properties) {
        return new RabbitMQConfig(createConnectionFactory(properties), properties);
    }

    protected void validateAddressProperties(Map<String, Object> properties) {
        validateNonNullProperty(properties, HOST_PROPERTY);
        validateNonNullProperty(properties, PORT_PROPERTY);
    }

    private static ConnectionFactory createConnectionFactory(Map<String, Object> properties) {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(Objects.toString(properties.get(HOST_PROPERTY)));
            connectionFactory.setPort(Integer.parseInt(Objects.toString(properties.get(PORT_PROPERTY))));
            connectionFactory.setVirtualHost(Objects.toString(properties.get(VIRTUAL_HOST_PROPERTY)));
            connectionFactory.setUsername(Objects.toString(properties.get(USERNAME_PROPERTY)));
            connectionFactory.setPassword(Objects.toString(properties.get(PASSWORD_PROPERTY)));
            if (!Objects.toString(properties.get(SSL_PROPERTY), DISABLED_CONFIG).equals(DISABLED_CONFIG)) {
                connectionFactory.useSslProtocol(Objects.toString(properties.get(SSL_PROPERTY)));
            }
            return connectionFactory;
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create ConnectionFactory: " + e);
        }
    }
}
