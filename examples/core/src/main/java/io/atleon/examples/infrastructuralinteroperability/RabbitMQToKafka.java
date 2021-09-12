package io.atleon.examples.infrastructuralinteroperability;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.StringBodyDeserializer;
import io.atleon.rabbitmq.StringBodySerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.function.Function;

/**
 * This example shows how an upstream RabbitMQ Queue can be processed to a downstream Kafka Topic.
 */
public class RabbitMQToKafka {

    private static final EmbeddedAmqpConfig EMBEDDED_AMQP_CONFIG = EmbeddedAmqp.start();

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String QUEUE = "QUEUE";

    private static final String TOPIC = "TOPIC";

    public static void main(String[] args) throws Exception {
        //Step 1) Create a RabbitMQ Config that we'll use for Publishing and Subscribing
        RabbitMQConfigSource rabbitMQConfig = RabbitMQConfigSource.named(RabbitMQToKafka.class.getSimpleName())
            .with(RabbitMQConfigSource.HOST_PROPERTY, EMBEDDED_AMQP_CONFIG.getHost())
            .with(RabbitMQConfigSource.PORT_PROPERTY, EMBEDDED_AMQP_CONFIG.getPort())
            .with(RabbitMQConfigSource.VIRTUAL_HOST_PROPERTY, EMBEDDED_AMQP_CONFIG.getVirtualHost())
            .with(RabbitMQConfigSource.USERNAME_PROPERTY, EMBEDDED_AMQP_CONFIG.getUsername())
            .with(RabbitMQConfigSource.PASSWORD_PROPERTY, EMBEDDED_AMQP_CONFIG.getPassword())
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());

        //Step 2) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, RabbitMQToKafka.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
            .with(ProducerConfig.ACKS_CONFIG, "all");

        //Step 3) Create Kafka Config for Consumer that backs Receiver. Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing with
        // our new consumer group
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, RabbitMQToKafka.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, RabbitMQToKafka.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 4) Producing to RabbitMQ requires that we declare a Queue to serve as the destination
        // and source of messages that we want to send/receive
        ConnectionFactory connectionFactory = rabbitMQConfig.createConnectionFactoryNow();
        try (Connection connection = connectionFactory.newConnection()) {
            connection.createChannel()
                .queueDeclare(QUEUE, false, false, false, null);
        }

        //Step 5) Produce some messages to the RabbitMQ Queue we declared
        AloRabbitMQSender.<String>from(rabbitMQConfig)
            .sendBodies(Flux.just("Test"), DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(QUEUE))
            .collectList()
            .doOnNext(outboundMessageResults -> System.out.println("outboundMessageResults: " + outboundMessageResults))
            .block();

        //Step 6) Apply a streaming process over a RabbitMQ -> Kafka pairing
        AloRabbitMQReceiver.<String>from(rabbitMQConfig)
            .receiveAloBodies(QUEUE)
            .map(String::toUpperCase)
            .transform(AloKafkaSender.<String, String>from(kafkaSenderConfig).sendAloValues(TOPIC, Function.identity()))
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(processedSenderResults -> System.out.println("processedSenderResults: " + processedSenderResults))
            .block();

        //Step 7) Consume the downstream results of the messages we processed
        AloKafkaReceiver.<String>forValues(kafkaReceiverConfig)
            .receiveAloValues(Collections.singletonList(TOPIC))
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(downstreamResults -> System.out.println("downstreamResults: " + downstreamResults))
            .block();

        System.exit(0);
    }
}
