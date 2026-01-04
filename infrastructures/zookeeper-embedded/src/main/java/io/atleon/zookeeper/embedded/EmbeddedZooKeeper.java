package io.atleon.zookeeper.embedded;

import java.net.URL;
import org.apache.curator.test.TestingServer;

public final class EmbeddedZooKeeper {

    public static final int DEFAULT_PORT = 2181;

    private static URL zooKeeperConnect;

    private EmbeddedZooKeeper() {}

    public static URL startAndGetConnectUrl() {
        return startAndGetConnectUrl(DEFAULT_PORT);
    }

    public static synchronized URL startAndGetConnectUrl(int port) {
        return zooKeeperConnect == null ? zooKeeperConnect = initializeZooKeeper(port) : zooKeeperConnect;
    }

    private static URL initializeZooKeeper(int port) {
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
