package io.atleon.spring;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootApplication
@SpringBootTest
@ActiveProfiles("config-sources")
class ConfigSourcesRegistrarTest {

    @Autowired
    private BeanFactory beanFactory;

    @Test
    public void debug() {
        assertDoesNotThrow(() -> beanFactory.getBean("kafkaConfigSource", KafkaConfigSource.class));
        assertDoesNotThrow(() -> beanFactory.getBean("rabbitMQConfigSource", RabbitMQConfigSource.class));
        assertDoesNotThrow(() -> beanFactory.getBean("snsConfigSource", SnsConfigSource.class));
        assertDoesNotThrow(() -> beanFactory.getBean("sqsConfigSource", SqsConfigSource.class));
    }
}
