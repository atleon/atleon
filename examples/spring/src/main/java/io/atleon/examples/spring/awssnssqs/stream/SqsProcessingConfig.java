package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.BodyDeserializer;
import io.atleon.aws.sqs.SqsConfig;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.aws.util.AwsConfig;
import io.atleon.core.AloStreamConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Map;
import java.util.function.Consumer;

@Component
public class SqsProcessingConfig implements AloStreamConfig {

    private final URI endpointOverride;

    private final String region;

    private final String accessKeyId;

    private final String secretAccessKey;

    private final String queueUrl;

    public SqsProcessingConfig(
        @Qualifier("sqsEndpointOverride") URI endpointOverride,
        @Qualifier("awsRegion") String region,
        @Qualifier("awsAccessKeyId") String accessKeyId,
        @Qualifier("awsSecretAccessKey") String secretAccessKey,
        @Qualifier("sqsQueueUrl") String queueUrl
    ) {
        this.endpointOverride = endpointOverride;
        this.region = region;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.queueUrl = queueUrl;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.unnamed()
            .with(SqsConfig.ENDPOINT_OVERRIDE_CONFIG, endpointOverride)
            .with(AwsConfig.REGION_CONFIG, region)
            .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
            .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, accessKeyId)
            .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, secretAccessKey)
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
        public void configure(Map<String, ?> properties) {

        }

        @Override
        public Long deserialize(String body) {
            return Long.parseLong(body);
        }
    }
}
