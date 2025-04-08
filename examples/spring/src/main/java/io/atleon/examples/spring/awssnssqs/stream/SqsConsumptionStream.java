package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

@AutoConfigureStream
public class SqsConsumptionStream extends SpringAloStream {

    private final SqsConfigSource configSource;

    private final NumbersService service;

    private final String queueUrl;

    public SqsConsumptionStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleSqsConfigSource", SqsConfigSource.class);
        this.service = context.getBean(NumbersService.class);
        this.queueUrl = context.getBean("sqsOutputQueueUrl", String.class);
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
