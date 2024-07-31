package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.core.ConfigContext;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

@AutoConfigureStream(KafkaConsumptionStream.class)
public class KafkaConsumptionStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public KafkaConsumptionStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withConsumerGroupId(KafkaConsumptionStreamConfig.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    public String getTopic() {
        return context.getProperty("stream.kafka.output.topic");
    }

    public NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
