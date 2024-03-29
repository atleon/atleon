package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.ComposedSnsMessage;
import io.atleon.core.AloStream;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

public class SnsGenerationStream extends AloStream<SnsGenerationStreamConfig> {

    @Override
    protected Disposable startDisposable(SnsGenerationStreamConfig config) {
        AloSnsSender<Long> sender = config.buildSender();

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendBodies(ComposedSnsMessage::fromBody, config.getTopicArn()))
            .doFinally(sender::close)
            .subscribe();
    }
}
