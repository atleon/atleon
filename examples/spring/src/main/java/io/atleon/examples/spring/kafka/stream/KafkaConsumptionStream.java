package io.atleon.examples.spring.kafka.stream;

import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

@AutoConfigureStream
public class KafkaConsumptionStream extends SpringAloStream {

    private final KafkaConfigSource configSource;

    private final NumbersService service;

    public KafkaConsumptionStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleKafkaConfigSource", KafkaConfigSource.class);
        this.service = context.getBean(NumbersService.class);
    }

    @Override
    protected Disposable startDisposable() {
        return buildKafkaLongReceiver()
            .receiveAloValues(getRequiredProperty("stream.kafka.output.topic"))
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
