package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;
import java.util.function.Consumer;

@AutoConfigureStream(KafkaProcessing.class)
public class KafkaProcessingConfig implements AloStreamConfig {

    private final Map<String, ?> kafkaProperties;

    private final String topic;

    public KafkaProcessingConfig(
        @Value("#{localKafka}") Map<String, ?> kafkaProperties,
        @Value("${example.kafka.topic}") String topic
    ) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(kafkaProperties)
            .withClientId(name())
            .withConsumerGroupId(KafkaProcessing.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .with(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
        return AloKafkaReceiver.from(configSource);
    }

    public String getTopic() {
        return topic;
    }

    public Consumer<String> getConsumer() {
        return System.out::println;
    }
}
