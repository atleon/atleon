package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.ConfigContext;

@AutoConfigureStream(SqsProcessingStream.class)
public class SqsProcessingStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public SqsProcessingStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloSqsReceiver.create(configSource);
    }

    public AloSqsSender<Long> buildSender() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
        return AloSqsSender.create(configSource);
    }

    public String getInputQueueUrl() {
        return context.getBean("sqsInputQueueUrl", String.class);
    }

    public String getOutputQueueUrl() {
        return context.getBean("sqsOutputQueueUrl", String.class);
    }

    public NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
