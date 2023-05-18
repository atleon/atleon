package io.atleon.kafka.avro.embedded;

import java.net.URL;

/**
 * @deprecated Use implementation from schema-registry-embedded (instead of kafka-avro-embedded)
 */
@Deprecated
public final class EmbeddedSchemaRegistry {

    private EmbeddedSchemaRegistry() {

    }

    public static URL startAndGetConnectUrl() {
        return io.atleon.schemaregistry.confluent.embedded.EmbeddedSchemaRegistry.startAndGetConnectUrl();
    }

    public static URL startAndGetConnectUrl(int port) {
        return io.atleon.schemaregistry.confluent.embedded.EmbeddedSchemaRegistry.startAndGetConnectUrl(port);
    }

    public static URL startAndGetConnectUrl(int port, int kafkaPort) {
        return io.atleon.schemaregistry.confluent.embedded.EmbeddedSchemaRegistry.startAndGetConnectUrl(port, kafkaPort);
    }

    public static  URL startAndGetConnectUrl(int port, int kafkaPort, int zookeeperPort) {
        return io.atleon.schemaregistry.confluent.embedded.EmbeddedSchemaRegistry.startAndGetConnectUrl(port, kafkaPort, zookeeperPort);
    }
}
