package io.atleon.examples.spring.rabbitmq;

import com.rabbitmq.client.ConnectionFactoryConfigurator;
import com.rabbitmq.client.MessageProperties;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessage;
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

    private static final EmbeddedAmqpConfig AMQP_CONFIG = EmbeddedAmqp.start(15672);

    private static final String EXCHANGE = "exchange";

    private static final String INPUT_QUEUE = "example-rabbitmq-input-queue";

    @Autowired
    private Consumer<Number> specialNumberConsumer; // Known mock from Test Configuration

    @Test
    public void primeNumbersAreProcessed() {
        long primeNumber = 10247693;

        produceNumber(primeNumber);

        verify(specialNumberConsumer, timeout(10000)).accept(eq(primeNumber));
    }

    private void produceNumber(Number number) {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.unnamed()
            .withAll(AMQP_CONFIG.asMap())
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class);
        try (AloRabbitMQSender<Long> sender = AloRabbitMQSender.from(configSource)) {
            RabbitMQMessage<Long> message =
                new RabbitMQMessage<>(EXCHANGE, INPUT_QUEUE, MessageProperties.MINIMAL_BASIC, number.longValue());
            sender.sendMessage(message).block();
        }
    }

    public static final class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                applicationContext,
                "example.rabbitmq." + ConnectionFactoryConfigurator.HOST + "=" + AMQP_CONFIG.getHost(),
                "example.rabbitmq." + ConnectionFactoryConfigurator.PORT + "=" + AMQP_CONFIG.getPort(),
                "example.rabbitmq." + ConnectionFactoryConfigurator.VIRTUAL_HOST + "=" + AMQP_CONFIG.getVirtualHost(),
                "example.rabbitmq." + ConnectionFactoryConfigurator.USERNAME + "=" + AMQP_CONFIG.getUsername(),
                "example.rabbitmq." + ConnectionFactoryConfigurator.PASSWORD + "=" + AMQP_CONFIG.getPassword(),
                "stream.rabbitmq.exchange=" + EXCHANGE,
                "stream.rabbitmq.input.queue=" + INPUT_QUEUE
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