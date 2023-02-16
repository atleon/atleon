package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

public class SqsProcessing extends AloStream<SqsProcessingConfig> {

    @Override
    protected Disposable startDisposable(SqsProcessingConfig config) {
        return config.buildReceiver()
            .receiveAloBodies(config.getQueueUrl())
            .filter(this::isPrime)
            .map(primeNumber -> "Found a prime number: " + primeNumber)
            .doOnNext(config.getConsumer())
            .resubscribeOnError(config.name())
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
