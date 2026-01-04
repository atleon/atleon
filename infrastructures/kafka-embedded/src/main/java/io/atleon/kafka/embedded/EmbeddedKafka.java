package io.atleon.kafka.embedded;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import org.apache.kafka.common.utils.Time;

public final class EmbeddedKafka {

    public static final int DEFAULT_PORT = 9092;

    private static EmbeddedKafkaConfig config;

    private EmbeddedKafka() {}

    public static String startAndGetBootstrapServersConnect() {
        return start(DEFAULT_PORT).getConnect();
    }

    public static String startAndGetBootstrapServersConnect(int port) {
        return start(port).getConnect();
    }

    public static EmbeddedKafkaConfig start() {
        return start(DEFAULT_PORT);
    }

    public static synchronized EmbeddedKafkaConfig start(int port) {
        return config == null ? config = initializeKafka(port) : config;
    }

    private static EmbeddedKafkaConfig initializeKafka(int port) {
        KafkaConfig kafkaConfig = new KafkaConfig(createKafkaBrokerConfig(port), true);
        startLocalKafka(kafkaConfig);
        return EmbeddedKafkaConfig.fromKafkaConfig(kafkaConfig);
    }

    /**
     * Using raw string keys to help maintain backward compatibility. See following classes for
     * hints on available properties:
     * <p>
     * <ul>
     *     <li>{@code org.apache.kafka.coordinator.group.GroupCoordinatorConfig}</li>
     *     <li>{@code org.apache.kafka.coordinator.transaction.TransactionLogConfigs}</li>
     *     <li>{@code org.apache.kafka.network.SocketServerConfigs}</li>
     *     <li>{@code org.apache.kafka.raft.QuorumConfig}</li>
     *     <li>{@code org.apache.kafka.server.config.KRaftConfigs}</li>
     *     <li>{@code org.apache.kafka.server.config.ReplicationConfigs}</li>
     *     <li>{@code org.apache.kafka.server.config.ServerLogConfigs}</li>
     * </ul>
     */
    private static Map<String, Object> createKafkaBrokerConfig(int port) {
        int controllerPort = port + 1;
        Map<String, Object> kafkaBrokerConfig = new HashMap<>();

        // GroupCoordinatorConfig
        kafkaBrokerConfig.put("offsets.topic.replication.factor", "1");

        // TransactionLogConfigs
        kafkaBrokerConfig.put("transaction.state.log.replication.factor", "1");
        kafkaBrokerConfig.put("transaction.state.log.min.isr", "1");

        // SocketServerConfigs
        kafkaBrokerConfig.put("advertised.listeners", "PLAINTEXT://localhost:" + port);
        kafkaBrokerConfig.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        kafkaBrokerConfig.put(
                "listeners", "PLAINTEXT://localhost:" + port + ",CONTROLLER://localhost:" + controllerPort);

        // QuorumConfig
        kafkaBrokerConfig.put("controller.quorum.voters", "1@localhost:" + controllerPort);

        // KRaftConfigs
        kafkaBrokerConfig.put("controller.listener.names", "CONTROLLER");
        kafkaBrokerConfig.put("node.id", 1);
        kafkaBrokerConfig.put("process.roles", "broker,controller");

        // ReplicationConfigs
        kafkaBrokerConfig.put("inter.broker.listener.name", "PLAINTEXT");

        // ServerLogConfigs
        kafkaBrokerConfig.put("log.dir", createLogDirectory().toString());
        kafkaBrokerConfig.put("num.partitions", 10);

        return kafkaBrokerConfig;
    }

    private static Path createLogDirectory() {
        try {
            Path path =
                    Files.createTempDirectory(EmbeddedKafka.class.getSimpleName() + "_" + System.currentTimeMillis());
            writeMetaProperties(path);
            return path;
        } catch (Exception e) {
            throw new IllegalStateException("Could not create temporary log directory", e);
        }
    }

    private static void writeMetaProperties(Path path) {
        try (Writer writer = Files.newBufferedWriter(path.resolve("meta.properties"), StandardCharsets.UTF_8)) {
            Properties properties = new Properties();
            properties.put("broker.id", "1");
            properties.put("cluster.id", "YCVE88x9RCiCfSDkQ0v9nQ");
            properties.store(writer, "KRaft meta properties");
        } catch (IOException e) {
            throw new IllegalStateException("Could not write meta.properties to log directory", e);
        }
    }

    private static void startLocalKafka(KafkaConfig kafkaConfig) {
        try {
            new KafkaRaftServer(kafkaConfig, Time.SYSTEM).startup();
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Kafka Server", e);
        }
    }
}
