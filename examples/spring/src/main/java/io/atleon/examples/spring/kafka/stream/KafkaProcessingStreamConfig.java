package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@AutoConfigureStream(KafkaProcessingStream.class)
public class KafkaProcessingStreamConfig implements AloStreamConfig {

    private final Map<String, ?> kafkaProperties;

    private final String inputTopic;

    private final String outputTopic;

    private final NumbersService service;

    public KafkaProcessingStreamConfig(
        @Qualifier("exampleKafkaProperties") Map<String, ?> kafkaProperties,
        @Value("${stream.kafka.input.topic}") String inputTopic,
        @Value("${stream.kafka.output.topic}") String outputTopic,
        NumbersService service
    ) {
        this.kafkaProperties = kafkaProperties;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.service = service;
    }

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = baseKafkaConfigSource()
            .withConsumerGroupId(KafkaProcessingStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    public AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = baseKafkaConfigSource()
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        return AloKafkaSender.create(configSource);
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public NumbersService getService() {
        return service;
    }

    private KafkaConfigSource baseKafkaConfigSource() {
        return KafkaConfigSource.useClientIdAsName()
            .withAll(kafkaProperties)
            .withClientId(name())
            .with(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());
    }
}
