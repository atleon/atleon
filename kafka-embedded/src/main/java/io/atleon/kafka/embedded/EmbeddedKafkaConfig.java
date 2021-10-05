package io.atleon.kafka.embedded;

import kafka.server.KafkaConfig;

import java.util.Map;
import java.util.Objects;

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
        return Objects.toString(kafkaConfigValues.get(KafkaConfig.InterBrokerSecurityProtocolProp()));
    }

    public String getConnect() {
        return Objects.toString(kafkaConfigValues.get(KafkaConfig.ListenersProp()));
    }
}
