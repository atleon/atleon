package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

import java.util.function.Function;

@AutoConfigureStream
public class KafkaProcessingStream extends SpringAloStream {

    private final KafkaConfigSource configSource;

    private final NumbersService service;

    public KafkaProcessingStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleKafkaConfigSource", KafkaConfigSource.class);
        this.service = context.getBean(NumbersService.class);
    }

    @Override
    protected Disposable startDisposable() {
        AloKafkaSender<Long, Long> sender = buildKafkaLongSender();
        String outputTopic = getRequiredProperty("stream.kafka.output.topic");

        return buildKafkaLongReceiver()
            .receiveAloRecords(getRequiredProperty("stream.kafka.input.topic"))
            .mapNotNull(ConsumerRecord::value)
            .filter(service::isPrime)
            .transform(sender.sendAloValues(outputTopic, Function.identity()))
            .resubscribeOnError(name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    private AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        return configSource.withClientId(name())
            .withConsumerGroupId(KafkaProcessingStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .as(AloKafkaReceiver::create);
    }

    private AloKafkaSender<Long, Long> buildKafkaLongSender() {
        return configSource.withClientId(name())
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class)
            .as(AloKafkaSender::create);
    }
}
