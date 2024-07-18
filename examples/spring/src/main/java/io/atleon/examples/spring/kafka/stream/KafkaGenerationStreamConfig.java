package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Profile("!integrationTest")
@AutoConfigureStream(KafkaGenerationStream.class)
public class KafkaGenerationStreamConfig implements AloStreamConfig {

    private final Map<String, ?> kafkaProperties;

    private final String topic;

    public KafkaGenerationStreamConfig(
        @Qualifier("exampleKafkaProperties") Map<String, ?> kafkaProperties,
        @Value("${stream.kafka.input.topic}") String topic
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
        return AloKafkaSender.create(configSource);
    }

    public String getTopic() {
        return topic;
    }
}
