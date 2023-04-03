package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.function.Consumer;

@AutoConfigureStream(KafkaProcessing.class)
public class KafkaProcessingConfig implements AloStreamConfig {

    private final KafkaConfigSource baseKafkaConfig;

    private final String topic;

    public KafkaProcessingConfig(
        @Qualifier("local") KafkaConfigSource localKafkaConfig,
        @Value("${example.kafka.topic}") String topic
    ) {
        this.baseKafkaConfig = localKafkaConfig;
        this.topic = topic;
    }

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource config = baseKafkaConfig.withClientId(name())
            .withConsumerGroupId(KafkaProcessing.class.getSimpleName())
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
        return System.out::println;
    }
}
