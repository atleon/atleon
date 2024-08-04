package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.ComposedSqsMessage;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import reactor.core.Disposable;

@AutoConfigureStream
public class SqsProcessingStream extends SelfConfigurableAloStream {

    private final SqsConfigSource configSource;

    private final NumbersService service;

    private final String inputQueueUrl;

    private final String outputQueueUrl;

    public SqsProcessingStream(
        SqsConfigSource exampleSqsConfigSource,
        NumbersService service,
        String sqsInputQueueUrl,
        String sqsOutputQueueUrl
    ) {
        this.configSource = exampleSqsConfigSource;
        this.service = service;
        this.inputQueueUrl = sqsInputQueueUrl;
        this.outputQueueUrl = sqsOutputQueueUrl;
    }

    @Override
    protected Disposable startDisposable() {
        AloSqsSender<Long> sender = buildSender();

        return buildReceiver()
            .receiveAloBodies(inputQueueUrl)
            .filter(service::isPrime)
            .transform(sender.sendAloBodies(ComposedSqsMessage::fromBody, outputQueueUrl))
            .resubscribeOnError(name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    private AloSqsReceiver<Long> buildReceiver() {
        return configSource.with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName())
            .as(AloSqsReceiver::create);
    }

    private AloSqsSender<Long> buildSender() {
        return configSource.with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .as(AloSqsSender::create);
    }
}
