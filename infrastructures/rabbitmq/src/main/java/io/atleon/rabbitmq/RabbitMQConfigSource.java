package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;
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

    public static RabbitMQConfigSource unnamed() {
        return new RabbitMQConfigSource();
    }

    public static RabbitMQConfigSource named(String name) {
        return new RabbitMQConfigSource(name);
    }

    public ConnectionFactory createConnectionFactoryNow() {
        return createConnectionFactory().block();
    }

    public Mono<ConnectionFactory> createConnectionFactory() {
        return create().map(RabbitMQConfig::buildConnectionFactory);
    }

    public RabbitMQConfigSource withHost(String host) {
        return with(ConnectionFactoryConfigurator.HOST, host);
    }

    public RabbitMQConfigSource withPort(int port) {
        return with(ConnectionFactoryConfigurator.PORT, port);
    }

    public RabbitMQConfigSource withVirtualHost(String virtualHost) {
        return with(ConnectionFactoryConfigurator.VIRTUAL_HOST, virtualHost);
    }

    public RabbitMQConfigSource withUsername(String username) {
        return with(ConnectionFactoryConfigurator.USERNAME, username);
    }

    public RabbitMQConfigSource withPassword(String password) {
        return with(ConnectionFactoryConfigurator.PASSWORD, password);
    }

    public RabbitMQConfigSource withSsl(String ssl) {
        return with(AloConnectionFactory.SSL, ssl);
    }

    @Override
    protected RabbitMQConfigSource initializeCopy() {
        return new RabbitMQConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateAnyNonNullProperty(properties, ConnectionFactoryConfigurator.HOST, AloConnectionFactory.HOSTS);
        validateAnyNonNullProperty(properties, ConnectionFactoryConfigurator.VIRTUAL_HOST, AloConnectionFactory.VHOST);
        validateNonNullProperty(properties, ConnectionFactoryConfigurator.USERNAME);
        validateNonNullProperty(properties, ConnectionFactoryConfigurator.PASSWORD);
    }

    @Override
    protected RabbitMQConfig postProcessProperties(Map<String, Object> properties) {
        return RabbitMQConfig.create(properties);
    }
}
