package io.atleon.examples.spring.kafka;

import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
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

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String INPUT_TOPIC = "example-kafka-input-topic";

    @Autowired
    private Consumer<Number> specialNumberConsumer; // Known mock from Test Configuration

    @Test
    public void primeNumbersAreProcessed() {
        long primeNumber = 10247693;

        produceNumber(primeNumber);

        verify(specialNumberConsumer, timeout(10000)).accept(eq(primeNumber));
    }

    private void produceNumber(Number number) {
        KafkaConfigSource configSource = KafkaConfigSource.useClientIdAsName()
            .withClientId("integration-test")
            .withBootstrapServers(BOOTSTRAP_SERVERS)
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class);
        try (AloKafkaSender<Long, Long> sender = AloKafkaSender.from(configSource)) {
            ProducerRecord<Long, Long> record = new ProducerRecord<>(INPUT_TOPIC, number.longValue(), number.longValue());
            sender.sendRecord(record).block();
        }
    }

    public static final class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                applicationContext,
                "example.kafka." + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + "=" + BOOTSTRAP_SERVERS,
                "stream.kafka.input.topic=" + INPUT_TOPIC
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
