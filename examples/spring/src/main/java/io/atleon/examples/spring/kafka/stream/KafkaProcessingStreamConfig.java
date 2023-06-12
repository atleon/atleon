package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@AutoConfigureStream(KafkaProcessingStream.class)
public class KafkaProcessingStreamConfig implements AloStreamConfig {

    private final Map<String, ?> kafkaProperties;

    private final String topic;

    private final NumbersService numbersService;

    public KafkaProcessingStreamConfig(
        @Qualifier("exampleKafkaProperties") Map<String, ?> kafkaProperties,
        @Value("${stream.kafka.input.topic}") String topic,
        NumbersService numbersService
    ) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.numbersService = numbersService;
    }

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(kafkaProperties)
            .withClientId(name())
            .withConsumerGroupId(KafkaProcessingStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .with(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
        return AloKafkaReceiver.from(configSource);
    }

    public String getTopic() {
        return topic;
    }

    public NumbersService getNumbersService() {
        return numbersService;
    }
}
