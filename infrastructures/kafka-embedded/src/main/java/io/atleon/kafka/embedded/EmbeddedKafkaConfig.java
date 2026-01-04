package io.atleon.kafka.embedded;

import java.util.Map;
import java.util.Objects;
import kafka.server.KafkaConfig;

public class EmbeddedKafkaConfig {

    private final Map<String, ?> kafkaConfigValues;

    private EmbeddedKafkaConfig(Map<String, ?> kafkaConfigValues) {
        this.kafkaConfigValues = kafkaConfigValues;
    }

    static EmbeddedKafkaConfig fromKafkaConfig(KafkaConfig kafkaConfig) {
        return new EmbeddedKafkaConfig(kafkaConfig.values());
    }

    public String getSecureConnect() {
        return getSecurityProtocol() + "://" + getConnect();
    }

    public String getSecurityProtocol() {
        // Config key from org.apache.kafka.server.config.ReplicationConfigs
        return Objects.toString(kafkaConfigValues.get("security.inter.broker.protocol"));
    }

    public String getConnect() {
        // Config key from org.apache.kafka.network.SocketServerConfigs
        return Objects.toString(kafkaConfigValues.get("advertised.listeners"));
    }
}
