package io.atleon.examples.spring.kafka.stream;

import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@AutoConfigureStream(KafkaGenerationStream.class)
public class KafkaGenerationStreamConfig extends SpringAloStreamConfig {

    public AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = getBean("exampleKafkaConfigSource", KafkaConfigSource.class)
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        return AloKafkaSender.create(configSource);
    }

    public String getTopic() {
        return getProperty("stream.kafka.input.topic");
    }
}
