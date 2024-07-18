package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Map;

@AutoConfigureStream(SqsConsumptionStream.class)
public class SqsConsumptionStreamConfig implements AloStreamConfig {

    private final Map<String, ?> awsProperties;

    private final String queueUrl;

    private final NumbersService service;

    public SqsConsumptionStreamConfig(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Qualifier("sqsOutputQueueUrl") String queueUrl,
        NumbersService service
    ) {
        this.awsProperties = awsProperties;
        this.queueUrl = queueUrl;
        this.service = service;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(awsProperties)
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class);
        return AloSqsReceiver.create(configSource);
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public NumbersService getService() {
        return service;
    }
}
