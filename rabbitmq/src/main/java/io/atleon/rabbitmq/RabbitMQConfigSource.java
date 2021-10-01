package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.core.ConfigSource;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class RabbitMQConfigSource extends ConfigSource<RabbitMQConfig, RabbitMQConfigSource> {

    /**
     * Hostname that may contain port in it. If it does, then 'port' property is ignored
     */
    public static final String HOST_PROPERTY = "host";

    public static final String PORT_PROPERTY = "port";

    public static final String VIRTUAL_HOST_PROPERTY = "virtual-host";

    public static final String USERNAME_PROPERTY = "username";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String SSL_PROPERTY = "ssl";

    public static final String DISABLED_CONFIG = "disabled";

    // Begin discouraged properties that will still be parsed
    /**
     * Comma-separated list of hosts. Only the first host is used when creating a ConnectionFactory
     */
    private static final String HOSTS_PROPERTY = "hosts";

    /**
     * Alias for 'virtual-host'
     */
    private static final String VHOST_PROPERTY = "vhost";

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
        return with(HOST_PROPERTY, host);
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
    protected RabbitMQConfigSource initializeCopy() {
        return new RabbitMQConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        HostAndPortConfig.validateProperties(properties);
        validateAnyNonNullProperty(properties, Arrays.asList(VIRTUAL_HOST_PROPERTY, VHOST_PROPERTY));
        validateNonNullProperty(properties, USERNAME_PROPERTY);
        validateNonNullProperty(properties, PASSWORD_PROPERTY);
    }

    @Override
    protected RabbitMQConfig postProcessProperties(Map<String, Object> properties) {
        return new RabbitMQConfig(createConnectionFactory(properties), properties);
    }

    private static ConnectionFactory createConnectionFactory(Map<String, Object> properties) {
        try {
            HostAndPortConfig hostAndPortConfig = HostAndPortConfig.parseFrom(properties);
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(hostAndPortConfig.getHost());
            connectionFactory.setPort(hostAndPortConfig.getPort());
            connectionFactory.setVirtualHost(extractVirtualHost(properties));
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

    private static String extractVirtualHost(Map<String, Object> properties) {
        Object virtualHost = properties.get(VIRTUAL_HOST_PROPERTY);
        return Objects.toString(virtualHost == null ? properties.get(VHOST_PROPERTY) : virtualHost);
    }

    private static final class HostAndPortConfig {

        private final String host;

        private final int port;

        private HostAndPortConfig(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public static HostAndPortConfig parseFrom(Map<String, Object> properties) {
            String[] splitHost = extractHost(properties).split(":");
            if (splitHost.length < 2) {
                int port = Optional.ofNullable(properties.get(PORT_PROPERTY))
                    .map(Object::toString)
                    .map(Integer::parseInt)
                    .orElse(ConnectionFactory.USE_DEFAULT_PORT);
                return new HostAndPortConfig(splitHost[0], port);
            } else {
                return new HostAndPortConfig(splitHost[0], Integer.parseInt(splitHost[1]));
            }
        }

        public static void validateProperties(Map<String, Object> properties) {
            extractHost(properties);
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        private static String extractHost(Map<String, Object> properties) {
            String host = Objects.toString(properties.get(HOST_PROPERTY), null);
            if (host != null) {
                return host;
            }
            String hosts = Objects.toString(properties.get(HOSTS_PROPERTY), null);
            if (hosts != null) {
                return hosts.split(",")[0];
            }
            throw new IllegalArgumentException("Host configuration missing from RabbitMQ properties=" + properties);
        }
    }
}
