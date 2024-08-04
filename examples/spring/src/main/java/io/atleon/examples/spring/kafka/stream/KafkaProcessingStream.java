package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.kafka.service.NumbersService;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.core.env.Environment;
import reactor.core.Disposable;

import java.util.function.Function;

@AutoConfigureStream
public class KafkaProcessingStream extends SelfConfigurableAloStream {

    private final KafkaConfigSource configSource;

    private final NumbersService service;

    private final Environment environment;

    public KafkaProcessingStream(
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
        AloKafkaSender<Long, Long> sender = buildKafkaLongSender();

        return buildKafkaLongReceiver()
            .receiveAloRecords(environment.getRequiredProperty("stream.kafka.input.topic"))
            .mapNotNull(ConsumerRecord::value)
            .filter(service::isPrime)
            .transform(sender.sendAloValues(outputTopic(), Function.identity()))
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

    private String outputTopic() {
        return environment.getRequiredProperty("stream.kafka.output.topic");
    }
}
