package io.atleon.examples.spring.kafka;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

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

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource config = baseKafkaConfig.withClientId(name())
            .withConsumerGroupId(ExampleKafkaProcessing.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .with(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
        return AloKafkaReceiver.from(config);
    }

    public String getTopic() {
        return topic;
    }

    public Consumer<String> getConsumer() {
        return consumer;
    }
}
