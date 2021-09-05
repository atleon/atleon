package io.atleon.examples.spring.kafka;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.KafkaConfigSource;

public class ExampleKafkaGenerationConfig implements AloStreamConfig {

    private final KafkaConfigSource baseKafkaConfig;

    private final String topic;

    public ExampleKafkaGenerationConfig(KafkaConfigSource baseKafkaConfig, String topic) {
        this.baseKafkaConfig = baseKafkaConfig;
        this.topic = topic;
    }

    @Override
    public String name() {
        return "kafka-generation";
    }

    public KafkaConfigSource getBaseKafkaConfig() {
        return baseKafkaConfig;
    }

    public String getTopic() {
        return topic;
    }
}
