package io.atleon.examples.spring.kafka;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.KafkaConfigSource;

import java.util.function.Consumer;

public class ExampleKafkaProcessingConfig implements AloStreamConfig {

    private final KafkaConfigSource baseKafkaConfig;

    private final String topic;

    private final Consumer<String> consumer;

    public ExampleKafkaProcessingConfig(KafkaConfigSource baseKafkaConfig, String topic, Consumer<String> consumer) {
        this.baseKafkaConfig = baseKafkaConfig;
        this.topic = topic;
        this.consumer = consumer;
    }

    @Override
    public String name() {
        return "kafka-processing";
    }

    public KafkaConfigSource getBaseKafkaConfig() {
        return baseKafkaConfig;
    }

    public String getTopic() {
        return topic;
    }

    public Consumer<String> getConsumer() {
        return consumer;
    }
}
