package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.aws.util.AwsConfig;
import io.atleon.core.AloStreamConfig;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;

import java.net.URI;

@AutoConfigureStream(SnsGeneration.class)
public class SnsGenerationConfig implements AloStreamConfig {

    private final URI endpointOverride;

    private final String region;

    private final String accessKeyId;

    private final String secretAccessKey;

    private final String topicArn;

    public SnsGenerationConfig(
        @Qualifier("snsEndpointOverride") URI endpointOverride,
        @Qualifier("awsRegion") String region,
        @Qualifier("awsAccessKeyId") String accessKeyId,
        @Qualifier("awsSecretAccessKey") String secretAccessKey,
        @Qualifier("snsTopicArn") String topicArn
    ) {
        this.endpointOverride = endpointOverride;
        this.region = region;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.topicArn = topicArn;
    }

    public AloSnsSender<Long> buildSender() {
        SnsConfigSource configSource = SnsConfigSource.unnamed()
            .with(SnsConfig.ENDPOINT_OVERRIDE_CONFIG, endpointOverride)
            .with(AwsConfig.REGION_CONFIG, region)
            .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
            .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, accessKeyId)
            .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, secretAccessKey)
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S");
        return AloSnsSender.from(configSource);
    }

    public String getTopicArn() {
        return topicArn;
    }
}
