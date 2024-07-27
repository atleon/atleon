package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@AutoConfigureStream(SnsGenerationStream.class)
public class SnsGenerationStreamConfig extends SpringAloStreamConfig {

    public AloSnsSender<Long> buildSender() {
        SnsConfigSource configSource = getBean("exampleAwsSnsConfigSource", SnsConfigSource.class)
            .rename(name())
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S");
        return AloSnsSender.create(configSource);
    }

    public String getTopicArn() {
        return getBean("snsInputTopicArn", String.class);
    }
}
