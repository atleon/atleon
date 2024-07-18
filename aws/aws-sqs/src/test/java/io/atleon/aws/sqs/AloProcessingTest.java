package io.atleon.aws.sqs;

import io.atleon.aws.util.AwsConfig;
import io.atleon.core.Alo;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class AloProcessingTest extends LocalStackDependentTest {

    @Test
    public void acknowledgedDataIsNotRepublished() {
        AloSqsSender.<String>create(newAloSqsSenderConfigSource())
            .sendBodies(Mono.just("DATA"), ComposedSqsMessage::fromBody, queueUrl)
            .then().block();

        AloSqsReceiver.create(newAloSqsReceiverConfigSource())
            .receiveAloBodies(queueUrl)
            .as(StepVerifier::create)
            .consumeNextWith(Alo::acknowledge)
            .thenCancel()
            .verify();

        AloSqsReceiver.create(newAloSqsReceiverConfigSource())
            .receiveAloBodies(queueUrl)
            .as(StepVerifier::create)
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10L))
            .thenCancel()
            .verify();
    }

    @Test
    public void unacknowledgedDataIsRepublished() {
        AloSqsSender.<String>create(newAloSqsSenderConfigSource())
            .sendBodies(Mono.just("DATA"), ComposedSqsMessage::fromBody, queueUrl)
            .then().block();

        AloSqsReceiver.create(newAloSqsReceiverConfigSource())
            .receiveAloBodies(queueUrl)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();

        AloSqsReceiver.create(newAloSqsReceiverConfigSource())
            .receiveAloBodies(queueUrl)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();
    }

    @Test
    public void nacknowledgedDataIsRepublished() {
        AloSqsSender.<String>create(newAloSqsSenderConfigSource())
            .sendBodies(Flux.just("DATA1", "DATA2", "DATA3"), ComposedSqsMessage::fromBody, queueUrl)
            .then().block();

        AloSqsReceiver.create(newAloSqsReceiverConfigSource())
            .receiveAloBodies(queueUrl)
            .resubscribeOnError(AloProcessingTest.class.getSimpleName(), Duration.ofSeconds(0L))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .consumeNextWith(alo -> Alo.nacknowledge(alo, new RuntimeException()))
            .expectNextCount(2)
            .thenCancel()
            .verify();
    }

    private SqsConfigSource newAloSqsSenderConfigSource() {
        return newAloSqsConfigSource()
            .with(AloSqsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
    }

    private SqsConfigSource newAloSqsReceiverConfigSource() {
        return newAloSqsConfigSource()
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());
    }

    private SqsConfigSource newAloSqsConfigSource() {
        return SqsConfigSource.unnamed()
            .with(SqsConfig.ENDPOINT_OVERRIDE_CONFIG, getSqsEndpointOverride())
            .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
            .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, getAccessKey())
            .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, getSecretKey())
            .with(AwsConfig.REGION_CONFIG, getRegion());
    }
}
