package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;

@AutoConfigureStream(SqsProcessingStream.class)
public class SqsProcessingStreamConfig extends SpringAloStreamConfig {

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = getBean("exampleAwsSqsConfigSource", SqsConfigSource.class)
            .rename(name())
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloSqsReceiver.create(configSource);
    }

    public AloSqsSender<Long> buildSender() {
        SqsConfigSource configSource = getBean("exampleAwsSqsConfigSource", SqsConfigSource.class)
            .rename(name())
            .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
        return AloSqsSender.create(configSource);
    }

    public String getInputQueueUrl() {
        return getBean("sqsInputQueueUrl", String.class);
    }

    public String getOutputQueueUrl() {
        return getBean("sqsOutputQueueUrl", String.class);
    }

    public NumbersService getService() {
        return getBean(NumbersService.class);
    }
}
