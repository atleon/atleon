package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import reactor.core.Disposable;

@AutoConfigureStream
public class SqsConsumptionStream extends SelfConfigurableAloStream {

    private final SqsConfigSource configSource;

    private final NumbersService service;

    private final String queueUrl;

    public SqsConsumptionStream(
        SqsConfigSource exampleSqsConfigSource,
        NumbersService service,
        String sqsOutputQueueUrl
    ) {
        this.configSource = exampleSqsConfigSource;
        this.service = service;
        this.queueUrl = sqsOutputQueueUrl;
    }

    @Override
    protected Disposable startDisposable() {
        return buildReceiver()
            .receiveAloBodies(queueUrl)
            .consume(service::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloSqsReceiver<Long> buildReceiver() {
        return configSource.with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class)
            .as(AloSqsReceiver::create);
    }
}
