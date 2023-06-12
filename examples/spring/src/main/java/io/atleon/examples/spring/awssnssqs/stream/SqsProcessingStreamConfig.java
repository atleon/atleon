package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Map;

@AutoConfigureStream(SqsProcessingStream.class)
public class SqsProcessingStreamConfig implements AloStreamConfig {

    private final Map<String, ?> awsProperties;

    private final String queueUrl;

    private final NumbersService numbersService;

    public SqsProcessingStreamConfig(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Qualifier("sqsInputQueueUrl") String queueUrl,
        NumbersService numbersService
    ) {
        this.awsProperties = awsProperties;
        this.queueUrl = queueUrl;
        this.numbersService = numbersService;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(awsProperties)
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloSqsReceiver.from(configSource);
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public NumbersService getNumbersService() {
        return numbersService;
    }
}
