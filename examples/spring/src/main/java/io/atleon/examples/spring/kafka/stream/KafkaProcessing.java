package io.atleon.examples.spring.kafka.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import io.atleon.kafka.AloKafkaReceiver;
import reactor.core.Disposable;

import java.util.Collections;

public class KafkaProcessing extends AloStream<KafkaProcessingConfig> {

    @Override
    protected Disposable startDisposable(KafkaProcessingConfig config) {
        AloKafkaReceiver<Long, Long> receiver = config.buildKafkaLongReceiver();

        return receiver.receiveAloValues(Collections.singletonList(config.getTopic()))
            .filter(this::isPrime)
            .map(primeNumber -> "Found a prime number: " + primeNumber)
            .doOnNext(config.getConsumer())
            .subscribe(Alo::acknowledge);
    }

    private boolean isPrime(Number number) {
        long safeValue = Math.abs(number.longValue());
        for (long i = 2; i <= Math.sqrt(safeValue); i++) {
            if (safeValue % i == 0) {
                return false;
            }
        }
        return safeValue > 0;
    }
}
