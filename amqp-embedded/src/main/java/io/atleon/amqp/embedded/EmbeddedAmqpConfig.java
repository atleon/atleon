package io.atleon.amqp.embedded;

import java.util.HashMap;
import java.util.Map;

public final class EmbeddedAmqpConfig {

    private final String host;

    private final int port;

    private final String virtualHost;

    private final String username;

    private final String password;

    EmbeddedAmqpConfig(String host, int port, String virtualHost, String username, String password) {
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.username = username;
        this.password = password;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("host", host);
        map.put("port", port);
        map.put("virtual-host", virtualHost);
        map.put("username", username);
        map.put("password", password);
        return map;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
