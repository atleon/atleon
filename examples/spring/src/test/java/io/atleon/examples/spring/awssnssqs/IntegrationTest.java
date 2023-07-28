package io.atleon.examples.spring.awssnssqs;

import io.atleon.aws.sns.AloSnsSender;
import io.atleon.aws.sns.ComposedSnsMessage;
import io.atleon.aws.sns.SnsAddress;
import io.atleon.aws.sns.SnsConfig;
import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sns.SnsMessage;
import io.atleon.aws.sns.StringBodySerializer;
import io.atleon.aws.sqs.SqsConfig;
import io.atleon.aws.testcontainers.AtleonLocalStackContainer;
import io.atleon.aws.util.AwsConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ContextConfiguration(initializers = IntegrationTest.Initializer.class)
@ActiveProfiles("integrationTest")
public class IntegrationTest {

    private static final AtleonLocalStackContainer CONTAINER = AtleonLocalStackContainer.createAndStart();

    private static final String SNS_INPUT_TOPIC_NAME = "example-sns-input-topic";

    private static final String SQS_INPUT_QUEUE_NAME = "example-sqs-input-queue";

    private static final String SQS_OUTPUT_QUEUE_NAME = "example-sqs-output-queue";

    @Autowired
    private String snsInputTopicArn; // Application creates this from set topic name, so just reuse it

    @Autowired
    private Consumer<Number> specialNumberConsumer; // Known mock from Test Configuration

    @Test
    public void primeNumbersAreProcessed() {
        long primeNumber = 10247693;

        produceNumber(primeNumber);

        verify(specialNumberConsumer, timeout(10000)).accept(eq(primeNumber));
    }

    private void produceNumber(Number number) {
        SnsConfigSource configSource = SnsConfigSource.unnamed()
            .with(AwsConfig.REGION_CONFIG, CONTAINER.getRegion())
            .with(AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG, AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC)
            .with(AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG, CONTAINER.getAccessKey())
            .with(AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG, CONTAINER.getSecretKey())
            .with(SnsConfig.ENDPOINT_OVERRIDE_CONFIG, CONTAINER.getSnsEndpointOverride())
            .with(AloSnsSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class);
        try (AloSnsSender<Long> sender = AloSnsSender.from(configSource)) {
            SnsMessage<Long> message = ComposedSnsMessage.fromBody(number.longValue());
            sender.sendMessage(message, SnsAddress.topicArn(snsInputTopicArn)).block();
        }
    }

    public static final class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                applicationContext,
                "example.aws.sns.sqs." + AwsConfig.REGION_CONFIG + "=" + CONTAINER.getRegion(),
                "example.aws.sns.sqs." + AwsConfig.CREDENTIALS_PROVIDER_TYPE_CONFIG + "=" + AwsConfig.CREDENTIALS_PROVIDER_TYPE_STATIC,
                "example.aws.sns.sqs." + AwsConfig.CREDENTIALS_ACCESS_KEY_ID_CONFIG + "=" + CONTAINER.getAccessKey(),
                "example.aws.sns.sqs." + AwsConfig.CREDENTIALS_SECRET_ACCESS_KEY_CONFIG + "=" + CONTAINER.getSecretKey(),
                "example.aws.sns.sqs." + SnsConfig.ENDPOINT_OVERRIDE_CONFIG + "=" + CONTAINER.getSnsEndpointOverride(),
                "example.aws.sns.sqs." + SqsConfig.ENDPOINT_OVERRIDE_CONFIG + "=" + CONTAINER.getSqsEndpointOverride(),
                "stream.sns.input.topic.name=" + SNS_INPUT_TOPIC_NAME,
                "stream.sqs.input.queue.name=" + SQS_INPUT_QUEUE_NAME,
                "stream.sqs.output.queue.name=" + SQS_OUTPUT_QUEUE_NAME
            );
        }
    }

    @TestConfiguration
    public static class Configuration {

        @Bean("specialNumberConsumer")
        public Consumer<Number> specialNumberConsumer() {
            return mock(Consumer.class);
        }
    }
}