package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.BodyDeserializer;
import io.atleon.aws.sqs.SqsConfig;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.AloStreamConfig;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.net.URI;
import java.util.Map;
import java.util.function.Consumer;

@AutoConfigureStream(SqsProcessing.class)
public class SqsProcessingConfig implements AloStreamConfig {

    private final Map<String, ?> localAwsProperties;

    private final URI endpointOverride;

    private final String queueUrl;

    public SqsProcessingConfig(
        @Value("#{localAws}") Map<String, ?> localAwsProperties,
        @Qualifier("localSqsEndpoint") URI endpointOverride,
        @Qualifier("sqsQueueUrl") String queueUrl
    ) {
        this.localAwsProperties = localAwsProperties;
        this.endpointOverride = endpointOverride;
        this.queueUrl = queueUrl;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(localAwsProperties)
            .with(SqsConfig.ENDPOINT_OVERRIDE_CONFIG, endpointOverride)
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloSqsReceiver.from(configSource);
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public Consumer<String> getConsumer() {
        return System.out::println;
    }

    public static final class LongBodyDeserializer implements BodyDeserializer<Long> {

        @Override
        public Long deserialize(String body) {
            return Long.parseLong(body);
        }
    }
}
