package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class SqsProcessingStream extends AloStream<SqsProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(SqsProcessingStreamConfig config) {
        return config.buildReceiver()
            .receiveAloBodies(config.getQueueUrl())
            .filter(config.getNumbersService()::isPrime)
            .resubscribeOnError(config.name())
            .subscribe(Alo::acknowledge);
    }
}
