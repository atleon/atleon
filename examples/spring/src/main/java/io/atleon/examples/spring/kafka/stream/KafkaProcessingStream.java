package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.ConfigContext;
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
import reactor.core.Disposable;

import java.util.function.Function;

@AutoConfigureStream
public class KafkaProcessingStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public KafkaProcessingStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    protected Disposable startDisposable() {
        AloKafkaSender<Long, Long> sender = buildKafkaLongSender();

        return buildKafkaLongReceiver()
            .receiveAloRecords(getInputTopic())
            .mapNotNull(ConsumerRecord::value)
            .filter(getService()::isPrime)
            .transform(sender.sendAloValues(getOutputTopic(), Function.identity()))
            .resubscribeOnError(name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    private AloKafkaReceiver<Long, Long> buildKafkaLongReceiver() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withConsumerGroupId(KafkaProcessingStream.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
        return AloKafkaReceiver.create(configSource);
    }

    private AloKafkaSender<Long, Long> buildKafkaLongSender() {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withAll(context.getPropertiesPrefixedBy("example.kafka"))
            .withClientId(name())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        return AloKafkaSender.create(configSource);
    }

    private String getInputTopic() {
        return context.getProperty("stream.kafka.input.topic");
    }

    private String getOutputTopic() {
        return context.getProperty("stream.kafka.output.topic");
    }

    private NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
