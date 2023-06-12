package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.core.AloStreamConfig;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Profile("!integrationTest")
@AutoConfigureStream(SnsGenerationStream.class)
public class SnsGenerationStreamConfig implements AloStreamConfig {

    private final Map<String, ?> awsProperties;

    private final String topicArn;

    public SnsGenerationStreamConfig(
        @Qualifier("exampleAwsSnsSqsProperties") Map<String, ?> awsProperties,
        @Qualifier("snsInputTopicArn") String topicArn
    ) {
        this.awsProperties = awsProperties;
        this.topicArn = topicArn;
    }

    public AloSnsSender<Long> buildSender() {
        SnsConfigSource configSource = SnsConfigSource.named(name())
            .withAll(awsProperties)
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S");
        return AloSnsSender.from(configSource);
    }

    public String getTopicArn() {
        return topicArn;
    }
}
