package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class RabbitMQProcessing extends AloStream<RabbitMQProcessingConfig> {

    @Override
    protected Disposable startDisposable(RabbitMQProcessingConfig config) {
        return config.buildRabbitMQLongReceiver()
            .receiveAloBodies(config.getQueue())
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
