package io.atleon.kafka;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.Test;

class SendingProducerTest {

    @Test
    public void invokeAndGet_givenPermittedMethodCalls_expectsReturnedResult() {
        MockProducer<String, String> mockProducer = new MockProducer<>();

        KafkaSenderOptions<String, String> options = KafkaSenderOptions.newBuilder(__ -> mockProducer)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        SendingProducer<String, String> producer = new SendingProducer<>(options);

        assertNotNull(producer.invokeAndGet(Producer::metrics).block());
    }

    @Test
    public void invoke_givenProhibitedMethodCall_expectsUnsupportedOperationException() {
        MockProducer<String, String> mockProducer = new MockProducer<>();

        KafkaSenderOptions<String, String> options = KafkaSenderOptions.newBuilder(__ -> mockProducer)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        SendingProducer<String, String> producer = new SendingProducer<>(options);

        assertThrows(UnsupportedOperationException.class, () -> producer.invoke(Producer::close)
                .block());
    }

    @Test
    public void invoke_givenRecursiveInvocation_expectsUnsupportedOperationException() {
        MockProducer<String, String> mockProducer = new MockProducer<>();

        KafkaSenderOptions<String, String> options = KafkaSenderOptions.newBuilder(__ -> mockProducer)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        SendingProducer<String, String> producer = new SendingProducer<>(options);

        assertThrows(
                UnsupportedOperationException.class,
                () -> producer.invoke(__ -> producer.invoke(Producer::metrics)).block());
    }

    @Test
    public void initTransactions_givenMultipleExecutions_expectsProducerInitializationOnce() {
        MockProducer<String, String> mockProducer = new MockProducer<>();

        KafkaSenderOptions<String, String> options = KafkaSenderOptions.newBuilder(__ -> mockProducer)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        SendingProducer<String, String> producer = new SendingProducer<>(options);

        producer.initTransactions().block();
        producer.initTransactions().block();

        assertTrue(mockProducer.transactionInitialized());
        assertFalse(mockProducer.closed());
    }

    @Test
    public void initTransactions_givenInitialFailureFollowedBySuccess_expectsProducerInitializationOnce() {
        MockProducer<String, String> mockProducer = new MockProducer<>();

        KafkaSenderOptions<String, String> options = KafkaSenderOptions.newBuilder(__ -> mockProducer)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        SendingProducer<String, String> producer = new SendingProducer<>(options);

        mockProducer.initTransactionException = new KafkaException("test");
        assertThrows(KafkaException.class, () -> producer.initTransactions().block());

        mockProducer.initTransactionException = null;
        producer.initTransactions().block();
        producer.initTransactions().block();

        assertTrue(mockProducer.transactionInitialized());
        assertFalse(mockProducer.closed());
    }

    @Test
    public void initTransactions_givenProducerFencedException_expectsProducerToBeClosed() {
        MockProducer<String, String> mockProducer = new MockProducer<>();

        mockProducer.initTransactionException = new ProducerFencedException("test");

        KafkaSenderOptions<String, String> options = KafkaSenderOptions.newBuilder(__ -> mockProducer)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        SendingProducer<String, String> producer = new SendingProducer<>(options);

        assertThrows(
                ProducerFencedException.class, () -> producer.initTransactions().block());
        assertNull(producer.closed().block());
        assertTrue(mockProducer.closed());
    }
}
