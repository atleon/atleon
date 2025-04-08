package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.ComposedSqsMessage;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

@AutoConfigureStream
public class SqsProcessingStream extends SpringAloStream {

    private final SqsConfigSource configSource;

    private final NumbersService service;

    private final String inputQueueUrl;

    private final String outputQueueUrl;

    public SqsProcessingStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("exampleSqsConfigSource", SqsConfigSource.class);
        this.service = context.getBean(NumbersService.class);
        this.inputQueueUrl = context.getBean("sqsInputQueueUrl", String.class);
        this.outputQueueUrl = context.getBean("sqsOutputQueueUrl", String.class);
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
