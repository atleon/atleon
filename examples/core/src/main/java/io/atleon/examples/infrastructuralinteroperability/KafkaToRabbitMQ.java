package io.atleon.examples.infrastructuralinteroperability;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.RabbitMQConfig;
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
import java.util.Map;
import java.util.function.Function;

/**
 * This example shows how an upstream Kafka Topic can be processed to a downstream RabbitMQ Queue.
 */
public class KafkaToRabbitMQ {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final Map<String, ?> TEST_AMQP_CONFIG = EmbeddedAmqp.start();

    private static final String TOPIC = "TOPIC";

    private static final String QUEUE = "QUEUE";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaToRabbitMQ.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
            .with(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create Kafka Config for Consumer that backs Receiver Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing
        // with our new consumer group
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaToRabbitMQ.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaToRabbitMQ.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Create a RabbitMQ Config that we'll use for Publishing and Subscribing
        RabbitMQConfigSource rabbitMQConfig = RabbitMQConfigSource.named(KafkaToRabbitMQ.class.getSimpleName())
            .with(RabbitMQConfigSource.HOST_PROPERTY, TEST_AMQP_CONFIG.get(EmbeddedAmqp.HOST_PROPERTY))
            .with(RabbitMQConfigSource.PORT_PROPERTY, TEST_AMQP_CONFIG.get(EmbeddedAmqp.PORT_PROPERTY))
            .with(RabbitMQConfigSource.VIRTUAL_HOST_PROPERTY, TEST_AMQP_CONFIG.get(EmbeddedAmqp.VIRTUAL_HOST_PROPERTY))
            .with(RabbitMQConfigSource.USERNAME_PROPERTY, TEST_AMQP_CONFIG.get(EmbeddedAmqp.USERNAME_PROPERTY))
            .with(RabbitMQConfigSource.PASSWORD_PROPERTY, TEST_AMQP_CONFIG.get(EmbeddedAmqp.PASSWORD_PROPERTY))
            .with(RabbitMQConfigSource.SSL_PROPERTY, TEST_AMQP_CONFIG.get(EmbeddedAmqp.SSL_PROPERTY))
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());

        //Step 4) Producing to RabbitMQ requires that we declare a Queue to serve as the destination
        // and source of messages that we want to send/receive
        ConnectionFactory connectionFactory = rabbitMQConfig.create()
            .map(RabbitMQConfig::getConnectionFactory)
            .block();
        try (Connection connection = connectionFactory.newConnection()) {
            connection.createChannel()
                .queueDeclare(QUEUE, false, false, false, null);
        }

        //Step 5) Produce some records to our Kafka topic
        AloKafkaSender.<String, String>from(kafkaSenderConfig)
            .sendValues(Flux.just("Test"), value -> TOPIC, Function.identity())
            .collectList()
            .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
            .block();

        //Step 6) Apply a streaming process over a Kafka -> RabbitMQ pairing
        AloKafkaReceiver.<String>forValues(kafkaReceiverConfig)
            .receiveAloValues(Collections.singletonList(TOPIC))
            .map(String::toUpperCase)
            .transform(AloRabbitMQSender.<String>from(rabbitMQConfig)
                .sendAloBodies(DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(QUEUE)))
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(processedMessageResults -> System.out.println("processedMessageResults: " + processedMessageResults))
            .block();

        //Step 7) Consume the downstream results of the messages we processed
        AloRabbitMQReceiver.<String>from(rabbitMQConfig)
            .receiveAloBodies(QUEUE)
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(downstreamResults -> System.out.println("downstreamResults: " + downstreamResults))
            .block();

        System.exit(0);
    }
}
