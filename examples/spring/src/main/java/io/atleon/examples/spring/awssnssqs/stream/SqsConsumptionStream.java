package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.core.AloStream;
import reactor.core.Disposable;

public class SqsConsumptionStream extends AloStream<SqsConsumptionStreamConfig> {

    @Override
    protected Disposable startDisposable(SqsConsumptionStreamConfig config) {
        return config.buildReceiver()
            .receiveAloBodies(config.getQueueUrl())
            .consume(config.getService()::handleNumber)
            .resubscribeOnError(config.name())
            .subscribe();
    }
}
