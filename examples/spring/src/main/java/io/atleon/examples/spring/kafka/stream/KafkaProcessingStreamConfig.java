package io.atleon.examples.spring.kafka.stream;

import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

@AutoConfigureStream(KafkaProcessingStream.class)
public class KafkaProcessingStreamConfig extends SpringAloStreamConfig {

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = getBean("exampleKafkaConfigSource", KafkaConfigSource.class)
            .withClientId(name())
            .withConsumerGroupId(KafkaProcessingStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    public AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = getBean("exampleKafkaConfigSource", KafkaConfigSource.class)
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        return AloKafkaSender.create(configSource);
    }

    public String getInputTopic() {
        return getProperty("stream.kafka.input.topic");
    }

    public String getOutputTopic() {
        return getProperty("stream.kafka.output.topic");
    }

    public NumbersService getService() {
        return getBean(NumbersService.class);
    }
}
