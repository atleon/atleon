package io.atleon.kafka.embedded;

import io.atleon.zookeeper.embedded.EmbeddedZooKeeper;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class EmbeddedKafka {

    public static final int DEFAULT_PORT = 9092;

    private static KafkaConfig kafkaConfig;

    private EmbeddedKafka() {

    }

    public static String startAndGetBootstrapServersConnect() {
        return extractConnect(start(DEFAULT_PORT));
    }

    public static String startAndGetBootstrapServersConnect(int port) {
        return extractConnect(start(port, EmbeddedZooKeeper.DEFAULT_PORT));
    }

    public static String startAndGetBootstrapServersConnect(int port, int zookeeperPort) {
        return extractConnect(start(port, zookeeperPort));
    }

    public static Map<String, ?> start() {
        return start(DEFAULT_PORT);
    }

    public static Map<String, ?> start(int port) {
        return start(port, EmbeddedZooKeeper.DEFAULT_PORT);
    }

    public static synchronized Map<String, ?> start(int port, int zookeeperPort) {
        KafkaConfig kafkaConfig = EmbeddedKafka.kafkaConfig == null
            ? EmbeddedKafka.kafkaConfig = initializeKafka(port, zookeeperPort)
            : EmbeddedKafka.kafkaConfig;
        return kafkaConfig.values();
    }

    public static String extractZookeeperConnect(Map<String, ?> kafkaConfig) {
        return Objects.toString(kafkaConfig.get(KafkaConfig.ZkConnectProp()));
    }

    public static String extractSecureConnect(Map<String, ?> kafkaConfig) {
        return extractSecurityProtocol(kafkaConfig) + "://" + extractConnect(kafkaConfig);
    }

    public static String extractSecurityProtocol(Map<String, ?> kafkaConfig) {
        return Objects.toString(kafkaConfig.get(KafkaConfig.InterBrokerSecurityProtocolProp()));
    }

    public static String extractConnect(Map<String, ?> kafkaConfig) {
        return kafkaConfig.get(KafkaConfig.HostNameProp()) + ":" + kafkaConfig.get(KafkaConfig.PortProp());
    }

    private static KafkaConfig initializeKafka(int port, int zookeeperPort) {
        URL zooKeeperConnect = EmbeddedZooKeeper.startAndGetConnectUrl(zookeeperPort);
        URL kafkaConnect = convertToConnectUrl("localhost:" + port);

        KafkaConfig kafkaConfig = new KafkaConfig(createKafkaBrokerConfig(zooKeeperConnect, kafkaConnect), true);
        startLocalKafka(kafkaConfig);
        return kafkaConfig;
    }

    private static URL convertToConnectUrl(String connect) {
        try {
            return new URL("http://" + connect);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create URL for Connect: " + connect, e);
        }
    }

    private static Map<String, Object> createKafkaBrokerConfig(URL zookeeperConnect, URL kafkaConnect) {
        Map<String, Object> kafkaBrokerConfig = new HashMap<>();
        kafkaBrokerConfig.put(KafkaConfig.ZkConnectProp(), extractConnect(zookeeperConnect));
        kafkaBrokerConfig.put(KafkaConfig.HostNameProp(), kafkaConnect.getHost());
        kafkaBrokerConfig.put(KafkaConfig.PortProp(), kafkaConnect.getPort());
        kafkaBrokerConfig.put(KafkaConfig.NumPartitionsProp(), 10);
        kafkaBrokerConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        kafkaBrokerConfig.put(KafkaConfig.LogDirProp(), createLogDirectory().toString());
        return kafkaBrokerConfig;
    }

    private static String extractConnect(URL connectUrl) {
        return connectUrl.getHost() + ":" + connectUrl.getPort();
    }

    private static Path createLogDirectory() {
        try {
            return Files.createTempDirectory(EmbeddedKafka.class.getSimpleName() + "_" + System.currentTimeMillis());
        } catch (Exception e) {
            throw new IllegalStateException("Could not create temporary log directory", e);
        }
    }

    private static void startLocalKafka(KafkaConfig kafkaConfig) {
        try {
            new KafkaServerStartable(kafkaConfig).startup();
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Kafka Server", e);
        }
    }
}
