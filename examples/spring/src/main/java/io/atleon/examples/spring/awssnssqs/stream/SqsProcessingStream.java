package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.ComposedSqsMessage;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.core.ConfigContext;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import reactor.core.Disposable;

@AutoConfigureStream
public class SqsProcessingStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public SqsProcessingStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    protected Disposable startDisposable() {
        AloSqsSender<Long> sender = buildSender();

        return buildReceiver()
            .receiveAloBodies(getInputQueueUrl())
            .filter(getService()::isPrime)
            .transform(sender.sendAloBodies(ComposedSqsMessage::fromBody, getOutputQueueUrl()))
            .resubscribeOnError(name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    private AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloSqsReceiver.create(configSource);
    }

    private AloSqsSender<Long> buildSender() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
        return AloSqsSender.create(configSource);
    }

    private String getInputQueueUrl() {
        return context.getBean("sqsInputQueueUrl", String.class);
    }

    private String getOutputQueueUrl() {
        return context.getBean("sqsOutputQueueUrl", String.class);
    }

    private NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
