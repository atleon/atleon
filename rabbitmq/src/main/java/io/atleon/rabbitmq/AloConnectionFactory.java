package io.atleon.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class AloConnectionFactory extends ConnectionFactory {

    /**
     * Host in the form of host[:port]. When port is specified in host, port property is ignored
     */
    public static final String HOST_PROPERTY = "host";

    /**
     * Comma-separated list of host[:port] pairs
     */
    public static final String HOSTS_PROPERTY = "hosts";

    /**
     * Used when port is not specified in a host's address
     */
    public static final String PORT_PROPERTY = "port";

    public static final String VIRTUAL_HOST_PROPERTY = "virtual-host";

    /**
     * Alias for {@link AloConnectionFactory#VIRTUAL_HOST_PROPERTY}
     */
    public static final String VHOST_PROPERTY = "vhost";

    public static final String USERNAME_PROPERTY = "username";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String SSL_PROPERTY = "ssl";

    public static final String DISABLED_CONFIG = "disabled";

    private final List<Address> addresses;

    public AloConnectionFactory(List<Address> addresses) {
        this.addresses = addresses;
    }

    public static AloConnectionFactory from(Map<String, ?> properties) {
        try {
            AloConnectionFactory connectionFactory = new AloConnectionFactory(extractAddresses(properties));
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
        int fallbackPort = Integer.parseInt(Objects.toString(properties.get(PORT_PROPERTY), "-1"));
        return Arrays.stream(Address.parseAddresses(hosts))
            .map(address -> address.getPort() == USE_DEFAULT_PORT && fallbackPort != USE_DEFAULT_PORT
                ? new Address(address.getHost(), fallbackPort) : address)
            .collect(Collectors.toList());
    }

    private static String extractHosts(Map<String, ?> properties) {
        Object host = properties.get(HOST_PROPERTY);
        return Objects.toString(host == null ? properties.get(HOSTS_PROPERTY) : host, DEFAULT_HOST);
    }

    private static String extractVirtualHost(Map<String, ?> properties) {
        Object virtualHost = properties.get(VIRTUAL_HOST_PROPERTY);
        return Objects.toString(virtualHost == null ? properties.get(VHOST_PROPERTY) : virtualHost, DEFAULT_VHOST);
    }
}
