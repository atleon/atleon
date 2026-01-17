package io.atleon.examples.infrastructuralinteroperability;

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
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RoutingInitializer;
import io.atleon.rabbitmq.StringBodyDeserializer;
import io.atleon.rabbitmq.StringBodySerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * This example shows how an upstream Kafka Topic can be processed to a downstream RabbitMQ Queue.
 */
public class KafkaToRabbitMQ {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final EmbeddedAmqpConfig EMBEDDED_AMQP_CONFIG = EmbeddedAmqp.start();

    private static final String TOPIC = "TOPIC";

    private static final String QUEUE = "QUEUE";

    public static void main(String[] args) throws Exception {
        // Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaToRabbitMQ.class.getSimpleName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) Produce some records to our Kafka topic
        AloKafkaSender<String, String> kafkaSender = AloKafkaSender.create(kafkaSenderConfig);
        kafkaSender
                .sendValues(Flux.just("Test"), value -> TOPIC, Function.identity())
                .collectList()
                .doOnNext(senderResults -> System.out.println("Sender results: " + senderResults))
                .doFinally(__ -> kafkaSender.close())
                .block();

        // Step 3) Create Kafka Config for Consumer that backs Receiver Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing
        // with our new consumer group
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaToRabbitMQ.class.getSimpleName())
                .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaToRabbitMQ.class.getSimpleName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Step 4) Create a RabbitMQ Config that we'll use for Publishing and Subscribing
        RabbitMQConfigSource rabbitMQConfig = RabbitMQConfigSource.named(KafkaToRabbitMQ.class.getSimpleName())
                .withHost(EMBEDDED_AMQP_CONFIG.getHost())
                .withPort(EMBEDDED_AMQP_CONFIG.getPort())
                .withVirtualHost(EMBEDDED_AMQP_CONFIG.getVirtualHost())
                .withUsername(EMBEDDED_AMQP_CONFIG.getUsername())
                .withPassword(EMBEDDED_AMQP_CONFIG.getPassword())
                .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
                .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());

        // Step 5) Producing to RabbitMQ requires that we declare a Queue to serve as the destination
        // and source of messages that we want to send/receive
        RoutingInitializer.using(rabbitMQConfig.createConnectionFactoryNow())
                .addQueueDeclaration(QueueDeclaration.named(QUEUE))
                .run();

        // Step 6) Apply a streaming process over a Kafka -> RabbitMQ pairing
        AloRabbitMQSender<String> rabbitMQSender = AloRabbitMQSender.create(rabbitMQConfig);
        AloKafkaReceiver.<Object, String>create(kafkaReceiverConfig)
                .receiveAloValues(TOPIC)
                .map(String::toUpperCase)
                .transform(rabbitMQSender.sendAloBodies(
                        DefaultRabbitMQMessageCreator.minimalBasicToDefaultExchange(QUEUE)))
                .consumeAloAndGet(Alo::acknowledge)
                .take(1)
                .collectList()
                .doOnNext(results -> System.out.println("Processed sender results: " + results))
                .doFinally(__ -> rabbitMQSender.close())
                .block();

        // Step 7) Consume the downstream results of the messages we processed
        AloRabbitMQReceiver.<String>create(rabbitMQConfig)
                .receiveAloBodies(QUEUE)
                .consumeAloAndGet(Alo::acknowledge)
                .take(1)
                .collectList()
                .doOnNext(downstreamResults -> System.out.println("Downstream results: " + downstreamResults))
                .block();

        System.exit(0);
    }
}
