package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;

@AutoConfigureStream(SqsConsumptionStream.class)
public class SqsConsumptionStreamConfig extends SpringAloStreamConfig {

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = getBean("exampleAwsSqsConfigSource", SqsConfigSource.class)
            .rename(name())
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class);
        return AloSqsReceiver.create(configSource);
    }

    public String getQueueUrl() {
        return getBean("sqsOutputQueueUrl", String.class);
    }

    public NumbersService getService() {
        return getBean(NumbersService.class);
    }
}
