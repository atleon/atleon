package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaGenerationConfig implements AloStreamConfig {

    private final KafkaConfigSource baseKafkaConfig;

    private final String topic;

    public KafkaGenerationConfig(
        @Qualifier("local") KafkaConfigSource localKafkaConfig,
        @Value("${example.kafka.topic}") String topic
    ) {
        this.baseKafkaConfig = localKafkaConfig;
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
