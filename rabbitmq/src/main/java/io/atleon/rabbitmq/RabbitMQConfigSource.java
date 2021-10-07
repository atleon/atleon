package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.core.ConfigSource;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class RabbitMQConfigSource extends ConfigSource<RabbitMQConfig, RabbitMQConfigSource> {

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

    public static RabbitMQConfigSource unnamed() {
        return new RabbitMQConfigSource();
    }

    public ConnectionFactory createConnectionFactoryNow() {
        return createConnectionFactory().block();
    }

    public Mono<ConnectionFactory> createConnectionFactory() {
        return create().map(RabbitMQConfig::getConnectionFactory);
    }

    public RabbitMQConfigSource withHost(String host) {
        return with(AloConnectionFactory.HOST_PROPERTY, host);
    }

    public RabbitMQConfigSource withPort(int port) {
        return with(AloConnectionFactory.PORT_PROPERTY, port);
    }

    public RabbitMQConfigSource withVirtualHost(String virtualHost) {
        return with(AloConnectionFactory.VIRTUAL_HOST_PROPERTY, virtualHost);
    }

    public RabbitMQConfigSource withUsername(String username) {
        return with(AloConnectionFactory.USERNAME_PROPERTY, username);
    }

    public RabbitMQConfigSource withPassword(String password) {
        return with(AloConnectionFactory.PASSWORD_PROPERTY, password);
    }

    public RabbitMQConfigSource withSsl(String ssl) {
        return with(AloConnectionFactory.SSL_PROPERTY, ssl);
    }

    @Override
    protected RabbitMQConfigSource initializeCopy() {
        return new RabbitMQConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateAnyNonNullProperty(properties,
            AloConnectionFactory.HOST_PROPERTY,
            AloConnectionFactory.HOSTS_PROPERTY
        );
        validateAnyNonNullProperty(properties,
            AloConnectionFactory.VIRTUAL_HOST_PROPERTY,
            AloConnectionFactory.VHOST_PROPERTY
        );
        validateNonNullProperty(properties, AloConnectionFactory.USERNAME_PROPERTY);
        validateNonNullProperty(properties, AloConnectionFactory.PASSWORD_PROPERTY);
    }

    @Override
    protected RabbitMQConfig postProcessProperties(Map<String, Object> properties) {
        return new RabbitMQConfig(AloConnectionFactory.from(properties), properties);
    }
}
