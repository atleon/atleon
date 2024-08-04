package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.ComposedSnsMessage;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.context.annotation.Profile;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@AutoConfigureStream
@Profile("!integrationTest")
public class SnsGenerationStream extends SelfConfigurableAloStream {

    private final SnsConfigSource configSource;

    private final String topicArn;

    public SnsGenerationStream(SnsConfigSource exampleRabbitMQConfigSource, String snsInputTopicArn) {
        this.configSource = exampleRabbitMQConfigSource;
        this.topicArn = snsInputTopicArn;
    }

    @Override
    protected Disposable startDisposable() {
        AloSnsSender<Long> sender = buildSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendBodies(ComposedSnsMessage::fromBody, topicArn))
            .doFinally(sender::close)
            .subscribe();
    }

    private AloSnsSender<Long> buildSender() {
        return configSource.rename(name())
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloSnsSender.BATCH_SIZE_CONFIG, 10)
            .with(AloSnsSender.BATCH_DURATION_CONFIG, "PT0.1S")
            .as(AloSnsSender::create);
    }
}
