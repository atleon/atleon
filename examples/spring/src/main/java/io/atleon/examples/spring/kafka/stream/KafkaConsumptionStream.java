package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.ConfigContext;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import reactor.core.Disposable;

@AutoConfigureStream
public class KafkaConsumptionStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public KafkaConsumptionStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    protected Disposable startDisposable() {
        return buildKafkaLongReceiver()
            .receiveAloValues(getTopic())
            .consume(getService()::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withConsumerGroupId(KafkaConsumptionStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    private String getTopic() {
        return context.getProperty("stream.kafka.output.topic");
    }

    private NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
