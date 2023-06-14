package io.atleon.examples.endtoendtoend;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * Part 2 of this example set illustrates how to both reactively produce and consume records from a
 * Kafka topic
 */
public class KafkaPart2 {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC = KafkaPart2.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart2.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .withProducerOrderingAndResiliencyConfigs();

        //Step 2) Create Kafka Config for Consumer that backs Receiver. Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing with
        // our new consumer group
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart2.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaPart2.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Send some Record values to a hardcoded topic, using values as Record keys
        AloKafkaSender.<String, String>from(kafkaReceiverConfig)
            .sendValues(Flux.just("Test"), TOPIC, Function.identity())
            .collectList()
            .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
            .block();

        //Step 4) Subscribe to the same topic we produced previous values to. Note that we must
        // specify how many values to process ('.take(1)'), or else this Flow would never complete
        AloKafkaReceiver.<String>forValues(kafkaSenderConfig)
            .receiveAloValues(TOPIC)
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(receivedValues -> System.out.println("receivedValues: " + receivedValues))
            .block();

        System.exit(0);
    }
}
