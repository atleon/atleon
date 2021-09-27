package io.atleon.examples.spring.kafka;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

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

    public AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource config = baseKafkaConfig.withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class)
            .with(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
        return AloKafkaSender.from(config);
    }

    public String getTopic() {
        return topic;
    }
}
