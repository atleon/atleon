package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.AloStream;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.kafka.AloKafkaSender;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.Disposable;

import java.util.function.Function;

public class KafkaProcessingStream extends AloStream<KafkaProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(KafkaProcessingStreamConfig config) {
        AloKafkaSender<Long, Long> sender = config.buildKafkaLongSender();

        return config.buildKafkaLongReceiver()
            .receiveAloRecords(config.getInputTopic())
            .mapNotNull(ConsumerRecord::value)
            .filter(config.getService()::isPrime)
            .transform(sender.sendAloValues(config.getOutputTopic(), Function.identity()))
            .resubscribeOnError(config.name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }
}
