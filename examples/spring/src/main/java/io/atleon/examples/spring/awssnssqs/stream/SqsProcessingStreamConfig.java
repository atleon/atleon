package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.AloSqsSender;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.sqs.StringBodySerializer;
import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Map;

@AutoConfigureStream(SqsProcessingStream.class)
public class SqsProcessingStreamConfig implements AloStreamConfig {

    private final Map<String, ?> awsProperties;

    private final String inputQueueUrl;

    private final String outputQueueUrl;

    private final NumbersService service;

    public SqsProcessingStreamConfig(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Qualifier("sqsInputQueueUrl") String inputQueueUrl,
        @Qualifier("sqsOutputQueueUrl") String outputQueueUrl,
        NumbersService service
    ) {
        this.awsProperties = awsProperties;
        this.inputQueueUrl = inputQueueUrl;
        this.outputQueueUrl = outputQueueUrl;
        this.service = service;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = baseSqsConfigSource()
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloSqsReceiver.from(configSource);
    }

    public AloSqsSender<Long> buildSender() {
        SqsConfigSource configSource = baseSqsConfigSource()
            .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
        return AloSqsSender.from(configSource);
    }

    public String getInputQueueUrl() {
        return inputQueueUrl;
    }

    public String getOutputQueueUrl() {
        return outputQueueUrl;
    }

    public NumbersService getService() {
        return service;
    }

    private SqsConfigSource baseSqsConfigSource() {
        return SqsConfigSource.named(name())
            .withAll(awsProperties);
    }
}
