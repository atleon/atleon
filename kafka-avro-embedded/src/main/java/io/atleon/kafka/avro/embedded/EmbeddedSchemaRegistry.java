package io.atleon.kafka.avro.embedded;

import io.atleon.kafka.embedded.EmbeddedKafka;
import io.atleon.kafka.embedded.EmbeddedKafkaConfig;
import io.atleon.zookeeper.embedded.EmbeddedZooKeeper;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class EmbeddedSchemaRegistry {

    public static final int DEFAULT_PORT = 8081;

    private static URL schemaRegistryConnect;

    private EmbeddedSchemaRegistry() {

    }

    public static URL startAndGetConnectUrl() {
        return startAndGetConnectUrl(DEFAULT_PORT);
    }

    public static URL startAndGetConnectUrl(int port) {
        return startAndGetConnectUrl(port, EmbeddedKafka.DEFAULT_PORT);
    }

    public static URL startAndGetConnectUrl(int port, int kafkaPort) {
        return startAndGetConnectUrl(port, kafkaPort, EmbeddedZooKeeper.DEFAULT_PORT);
    }

    public static synchronized URL startAndGetConnectUrl(int port, int kafkaPort, int zookeeperPort) {
        return schemaRegistryConnect == null
            ? schemaRegistryConnect = initializeSchemaRegistry(port, kafkaPort, zookeeperPort)
            : schemaRegistryConnect;
    }

    private static URL initializeSchemaRegistry(int port, int kafkaPort, int zookeeperPort) {
        try {
            URL schemaConnect = convertToConnectUrl("localhost:" + port);
            EmbeddedKafkaConfig embeddedKafkaConfig = EmbeddedKafka.start(kafkaPort, zookeeperPort);
            Properties registryProperties = new Properties();
            registryProperties.putAll(createLocalSchemaRegistryConfig(schemaConnect, embeddedKafkaConfig));
            new SchemaRegistryRestApplication(new SchemaRegistryConfig(registryProperties)).start();
            return schemaConnect;
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Schema App", e);
        }
    }

    private static URL convertToConnectUrl(String connect) {
        try {
            return new URL("http://" + connect);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create URL for Connect: " + connect, e);
        }
    }

    private static Map<String, Object> createLocalSchemaRegistryConfig(URL schemaConnect, EmbeddedKafkaConfig embeddedKafkaConfig) {
        Map<String, Object> registryConfig = new HashMap<>();
        registryConfig.put(SchemaRegistryConfig.LISTENERS_CONFIG, schemaConnect.toString());
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG, embeddedKafkaConfig.getSecurityProtocol());
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaConfig.getSecureConnect());
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas");
        return registryConfig;
    }
}
