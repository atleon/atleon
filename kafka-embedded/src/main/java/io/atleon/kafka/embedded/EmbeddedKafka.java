package io.atleon.kafka.embedded;

import io.atleon.zookeeper.embedded.EmbeddedZooKeeper;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class EmbeddedKafka {

    public static final int DEFAULT_PORT = 9092;

    private static EmbeddedKafkaConfig config;

    private EmbeddedKafka() {

    }

    public static String startAndGetBootstrapServersConnect() {
        return start(DEFAULT_PORT).getConnect();
    }

    public static String startAndGetBootstrapServersConnect(int port) {
        return start(port, EmbeddedZooKeeper.DEFAULT_PORT).getConnect();
    }

    public static String startAndGetBootstrapServersConnect(int port, int zookeeperPort) {
        return start(port, zookeeperPort).getConnect();
    }

    public static EmbeddedKafkaConfig start() {
        return start(DEFAULT_PORT);
    }

    public static EmbeddedKafkaConfig start(int port) {
        return start(port, EmbeddedZooKeeper.DEFAULT_PORT);
    }

    public static synchronized EmbeddedKafkaConfig start(int port, int zookeeperPort) {
        return config == null ? config = initializeKafka(port, zookeeperPort) : config;
    }

    private static EmbeddedKafkaConfig initializeKafka(int port, int zookeeperPort) {
        URL zooKeeperConnect = EmbeddedZooKeeper.startAndGetConnectUrl(zookeeperPort);
        KafkaConfig kafkaConfig = new KafkaConfig(createKafkaBrokerConfig(zooKeeperConnect, port), true);
        startLocalKafka(kafkaConfig);
        return EmbeddedKafkaConfig.fromKafkaConfig(kafkaConfig);
    }

    private static Map<String, Object> createKafkaBrokerConfig(URL zookeeperConnect, int port) {
        Map<String, Object> kafkaBrokerConfig = new HashMap<>();
        kafkaBrokerConfig.put(KafkaConfig.ZkConnectProp(), extractConnect(zookeeperConnect));
        kafkaBrokerConfig.put(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:" + port);
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
