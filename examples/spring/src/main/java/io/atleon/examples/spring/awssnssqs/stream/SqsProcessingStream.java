package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.ComposedSqsMessage;
import io.atleon.core.AloStream;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import reactor.core.Disposable;

public class SqsProcessingStream extends AloStream<SqsProcessingStreamConfig> {

    @Override
    protected Disposable startDisposable(SqsProcessingStreamConfig config) {
        AloSqsSender<Long> sender = config.buildSender();

        return config.buildReceiver()
            .receiveAloBodies(config.getInputQueueUrl())
            .filter(config.getService()::isPrime)
            .transform(sender.sendAloBodies(ComposedSqsMessage::fromBody, config.getOutputQueueUrl()))
            .resubscribeOnError(config.name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }
}
