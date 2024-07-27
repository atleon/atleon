package io.atleon.examples.spring.kafka.stream;

import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

@AutoConfigureStream(KafkaConsumptionStream.class)
public class KafkaConsumptionStreamConfig extends SpringAloStreamConfig {

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = getBean("exampleKafkaConfigSource", KafkaConfigSource.class)
            .withClientId(name())
            .withConsumerGroupId(KafkaConsumptionStreamConfig.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    public String getTopic() {
        return getProperty("stream.kafka.output.topic");
    }

    public NumbersService getService() {
        return getBean(NumbersService.class);
    }
}
