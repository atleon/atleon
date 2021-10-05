package io.atleon.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * ConnectionFactory extension with support for multiple hosts and instantiation via properties.
 * Configuration properties are the same ones defined by {@link ConnectionFactoryConfigurator}
 */
public class AloConnectionFactory extends ConnectionFactory {

    /**
     * Comma-separated list of host[:port] pairs
     */
    public static final String HOSTS = "hosts";

    /**
     * Alias for {@link ConnectionFactoryConfigurator#VIRTUAL_HOST}
     */
    public static final String VHOST = "vhost";

    /**
     * Configuration for ssl. Can be set to {@link AloConnectionFactory#DISABLED} or protocol
     */
    public static final String SSL = "ssl";

    public static final String DISABLED = "disabled";

    private final List<Address> addresses;

    public AloConnectionFactory(List<Address> addresses) {
        this.addresses = addresses;
    }

    public static AloConnectionFactory from(Map<String, ?> properties) {
        List<Address> addresses = extractAddresses(properties);

        Map<String, String> configuratorProperties = createConfiguratorProperties(properties);
        // Remove Host and Port configuration since we handle addresses manually
        configuratorProperties.remove(ConnectionFactoryConfigurator.HOST);
        configuratorProperties.remove(ConnectionFactoryConfigurator.PORT);

        AloConnectionFactory connectionFactory = new AloConnectionFactory(addresses);
        ConnectionFactoryConfigurator.load(connectionFactory, configuratorProperties, "");
        return connectionFactory;
    }

    @Override
    public Connection newConnection() throws IOException, TimeoutException {
        return newConnection(addresses);
    }

    @Override
    public Connection newConnection(String connectionName) throws IOException, TimeoutException {
        return newConnection(addresses, connectionName);
    }

    @Override
    public Connection newConnection(ExecutorService executor) throws IOException, TimeoutException {
        return newConnection(executor, addresses);
    }

    @Override
    public Connection newConnection(ExecutorService executor, String connectionName) throws IOException, TimeoutException {
        return newConnection(executor, addresses, connectionName);
    }

    @Override
    public String getHost() {
        return addresses.get(0).getHost();
    }

    @Override
    public void setHost(String host) {
        throw new UnsupportedOperationException("Cannot set host on multi-address ConnectionFactory");
    }

    @Override
    public int getPort() {
        return addresses.get(0).getPort();
    }

    @Override
    public void setPort(int port) {
        throw new UnsupportedOperationException("Cannot set port on multi-address ConnectionFactory");
    }

    private static List<Address> extractAddresses(Map<String, ?> properties) {
        String hosts = extractHosts(properties);
        int fallbackPort = Optional.<Object>ofNullable(properties.get(ConnectionFactoryConfigurator.PORT))
            .map(port -> Integer.parseInt(port.toString()))
            .orElse(USE_DEFAULT_PORT);
        return Arrays.stream(Address.parseAddresses(hosts))
            .map(address -> address.getPort() == USE_DEFAULT_PORT && fallbackPort != USE_DEFAULT_PORT
                ? new Address(address.getHost(), fallbackPort) : address)
            .collect(Collectors.toList());
    }

    private static String extractHosts(Map<String, ?> properties) {
        Object host = properties.get(ConnectionFactoryConfigurator.HOST);
        return Objects.toString(host == null ? properties.get(HOSTS) : host, DEFAULT_HOST);
    }

    private static Map<String, String> createConfiguratorProperties(Map<String, ?> properties) {
        Map<String, String> configuration = properties.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> Objects.toString(entry.getValue())));

        if (!configuration.containsKey(ConnectionFactoryConfigurator.VIRTUAL_HOST) && configuration.containsKey(VHOST)) {
            configuration.put(ConnectionFactoryConfigurator.VIRTUAL_HOST, configuration.remove(VHOST));
        }

        String ssl = configuration.getOrDefault(SSL, DISABLED);
        if (!configuration.containsKey(ConnectionFactoryConfigurator.SSL_ENABLED) && !ssl.equals(DISABLED)) {
            configuration.put(ConnectionFactoryConfigurator.SSL_ENABLED, "true");
            configuration.put(ConnectionFactoryConfigurator.SSL_ALGORITHM, ssl);
        }

        return configuration;
    }
}
