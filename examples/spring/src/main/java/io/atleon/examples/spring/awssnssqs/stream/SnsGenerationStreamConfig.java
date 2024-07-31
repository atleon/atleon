package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.core.AloStreamConfig;
import io.atleon.core.ConfigContext;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@AutoConfigureStream(SnsGenerationStream.class)
public class SnsGenerationStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public SnsGenerationStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloSnsSender<Long> buildSender() {
        SnsConfigSource configSource = SnsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S");
        return AloSnsSender.create(configSource);
    }

    public String getTopicArn() {
        return context.getBean("snsInputTopicArn", String.class);
    }
}
