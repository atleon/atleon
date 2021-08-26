package io.atleon.zookeeper.embedded;

import org.apache.curator.test.TestingServer;

import java.net.URL;

public final class EmbeddedZookeeper {

    public static final int DEFAULT_PORT = 2181;

    private static URL zookeeperConnect;

    private EmbeddedZookeeper() {

    }

    public static URL startAndGetConnectUrl() {
        return startAndGetConnectUrl(DEFAULT_PORT);
    }

    public static synchronized URL startAndGetConnectUrl(int port) {
        return zookeeperConnect == null ? zookeeperConnect = initializeZookeeper(port) : zookeeperConnect;
    }

    private static URL initializeZookeeper(int port) {
        try {
            new TestingServer(port);
            return convertToConnectUrl("localhost:" + port);
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Zookeeper Server", e);
        }
    }

    private static URL convertToConnectUrl(String connect) {
        try {
            return new URL("http://" + connect);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create URL for Connect: " + connect, e);
        }
    }
}
