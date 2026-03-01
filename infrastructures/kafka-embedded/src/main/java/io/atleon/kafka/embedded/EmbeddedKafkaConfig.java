package io.atleon.kafka.embedded;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
        Object advertisedListeners = kafkaConfigValues.get("advertised.listeners");
        Collection<?> advertisedListenerCollection = advertisedListeners instanceof Collection
                ? Collection.class.cast(advertisedListeners)
                : Collections.singletonList(advertisedListeners);
        return advertisedListenerCollection.stream().map(Objects::toString).collect(Collectors.joining(","));
    }
}
