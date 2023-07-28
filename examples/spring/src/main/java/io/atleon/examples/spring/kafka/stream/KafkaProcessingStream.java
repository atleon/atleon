package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import io.atleon.kafka.AloKafkaSender;
import reactor.core.Disposable;

import java.util.function.Function;

public class KafkaProcessingStream extends AloStream<KafkaProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(KafkaProcessingStreamConfig config) {
        AloKafkaSender<Long, Long> sender = config.buildKafkaLongSender();

        return config.buildKafkaLongReceiver()
            .receiveAloValues(config.getInputTopic())
            .filter(config.getService()::isPrime)
            .transform(sender.sendAloValues(config.getOutputTopic(), Function.identity()))
            .resubscribeOnError(config.name())
            .doFinally(sender::close)
            .subscribe(Alo::acknowledge);
    }
}
