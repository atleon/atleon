package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.core.env.Environment;
import reactor.core.Disposable;

@AutoConfigureStream
public class KafkaConsumptionStream extends SelfConfigurableAloStream {

    private final KafkaConfigSource configSource;

    private final NumbersService service;

    private final Environment environment;

    public KafkaConsumptionStream(
        KafkaConfigSource exampleKafkaConfigSource,
        NumbersService service,
        Environment environment
    ) {
        this.configSource = exampleKafkaConfigSource;
        this.service = service;
        this.environment = environment;
    }

    @Override
    protected Disposable startDisposable() {
        return buildKafkaLongReceiver()
            .receiveAloValues(environment.getRequiredProperty("stream.kafka.output.topic"))
            .consume(service::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        return configSource.withClientId(name())
            .withConsumerGroupId(KafkaConsumptionStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .as(AloKafkaReceiver::create);
    }
}
