package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.core.ConfigContext;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

@AutoConfigureStream(KafkaProcessingStream.class)
public class KafkaProcessingStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public KafkaProcessingStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withConsumerGroupId(KafkaProcessingStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    public AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        return AloKafkaSender.create(configSource);
    }

    public String getInputTopic() {
        return context.getProperty("stream.kafka.input.topic");
    }

    public String getOutputTopic() {
        return context.getProperty("stream.kafka.output.topic");
    }

    public NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
