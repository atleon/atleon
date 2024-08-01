package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.ComposedSnsMessage;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.core.ConfigContext;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@AutoConfigureStream
@Profile("!integrationTest")
public class SnsGenerationStream extends SelfConfigurableAloStream {

    private final ConfigContext context;

    public SnsGenerationStream(ConfigContext context) {
        this.context = context;
    }

    @Override
    protected Disposable startDisposable() {
        AloSnsSender<Long> sender = buildSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendBodies(ComposedSnsMessage::fromBody, getTopicArn()))
            .doFinally(sender::close)
            .subscribe();
    }

    private AloSnsSender<Long> buildSender() {
        SnsConfigSource configSource = SnsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S");
        return AloSnsSender.create(configSource);
    }

    private String getTopicArn() {
        return context.getBean("snsInputTopicArn", String.class);
    }
}
