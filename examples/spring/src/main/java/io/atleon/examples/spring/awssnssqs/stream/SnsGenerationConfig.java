package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.core.AloStreamConfig;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.net.URI;
import java.util.Map;

@AutoConfigureStream(SnsGeneration.class)
public class SnsGenerationConfig implements AloStreamConfig {

    private final Map<String, ?> localAwsProperties;

    private final URI endpointOverride;

    private final String topicArn;

    public SnsGenerationConfig(
        @Value("#{localAws}") Map<String, ?> localAwsProperties,
        @Qualifier("localSnsEndpoint") URI endpointOverride,
        @Qualifier("snsTopicArn") String topicArn
    ) {
        this.localAwsProperties = localAwsProperties;
        this.endpointOverride = endpointOverride;
        this.topicArn = topicArn;
    }

    public AloSnsSender<Long> buildSender() {
        SnsConfigSource configSource = SnsConfigSource.named(name())
            .withAll(localAwsProperties)
            .with(SnsConfig.ENDPOINT_OVERRIDE_CONFIG, endpointOverride)
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S");
        return AloSnsSender.from(configSource);
    }

    public String getTopicArn() {
        return topicArn;
    }
}
