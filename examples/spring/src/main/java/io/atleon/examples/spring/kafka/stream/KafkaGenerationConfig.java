package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@AutoConfigureStream(KafkaGeneration.class)
public class KafkaGenerationConfig implements AloStreamConfig {

    private final Map<String, ?> kafkaProperties;

    private final String topic;

    public KafkaGenerationConfig(
        @Value("#{localKafka}") Map<String, ?> kafkaProperties,
        @Value("${example.kafka.topic}") String topic
    ) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    public AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(kafkaProperties)
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class)
            .with(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
        return AloKafkaSender.from(configSource);
    }

    public String getTopic() {
        return topic;
    }
}
