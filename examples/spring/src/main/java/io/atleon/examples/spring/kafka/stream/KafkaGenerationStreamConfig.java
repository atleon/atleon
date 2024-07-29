package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.ConfigContext;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@AutoConfigureStream(KafkaGenerationStream.class)
public class KafkaGenerationStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public KafkaGenerationStreamConfig(ConfigContext context) {
        this.context = context;
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

    public String getTopic() {
        return context.getProperty("stream.kafka.input.topic");
    }
}
