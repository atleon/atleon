package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.ConfigContext;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import reactor.core.Disposable;

@AutoConfigureStream
public class SqsConsumptionStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public SqsConsumptionStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    protected Disposable startDisposable() {
        return buildReceiver()
            .receiveAloBodies(getQueueUrl())
            .consume(getService()::handleNumber)
            .resubscribeOnError(name())
            .subscribe();
    }

    private AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class);
        return AloSqsReceiver.create(configSource);
    }

    private String getQueueUrl() {
        return context.getBean("sqsOutputQueueUrl", String.class);
    }

    private NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
