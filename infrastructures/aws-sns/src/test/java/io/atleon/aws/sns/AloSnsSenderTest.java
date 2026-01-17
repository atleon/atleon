package io.atleon.aws.sns;

import io.atleon.aws.util.AwsConfig;
import io.atleon.core.Alo;
import io.atleon.core.ComposedAlo;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloSnsSenderTest extends LocalStackDependentTest {

    @Test
    public void aloCallbacksOnResultsArePropagatedBackToOriginatingData() {
        AtomicBoolean acknowledged = new AtomicBoolean(false);
        AtomicReference<Throwable> nacknowledgement = new AtomicReference<>(null);

        Alo<SnsMessage<String>> aloSnsMessage = new ComposedAlo<>(
                ComposedSnsMessage.fromBody(UUID.randomUUID().toString()),
                () -> acknowledged.set(true),
                nacknowledgement::set);

        AloSnsSender.<String>create(newAloSnsSenderConfigSource())
                .sendAloMessages(Flux.just(aloSnsMessage), topicArn)
                .consumeAloAndGet(alo -> {
                    Alo.acknowledge(alo);
                    Alo.nacknowledge(alo, new RuntimeException());
                })
                .then()
                .block();

        assertTrue(acknowledged.get());
        assertNotNull(nacknowledgement.get());
    }

    private SnsConfigSource newAloSnsSenderConfigSource() {
        return SnsConfigSource.unnamed()
                .with(SnsConfig.ENDPOINT_OVERRIDE_CONFIG, getSqsEndpointOverride())
                .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
                .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, getAccessKey())
                .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, getSecretKey())
                .with(AwsConfig.REGION_CONFIG, getRegion())
                .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
    }
}
