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

import java.util.Collections;
import java.util.function.Function;

/**
 * Part 4 of this sample set extends Part 3 to consume the downstream results of the streaming
 * process we added
 */
public class KafkaPart4 {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = KafkaPart4.class.getSimpleName() + "-1";

    private static final String TOPIC_2 = KafkaPart4.class.getSimpleName() + "-2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = new KafkaConfigSource()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart4.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
            .with(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create Kafka Config for Consumer that backs Receiver. Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing with
        // our new consumer group
        KafkaConfigSource kafkaReceiverConfig = new KafkaConfigSource()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart4.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaPart4.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Create a Sender which we'll reuse to produce Records
        AloKafkaSender<Object, String> sender = AloKafkaSender.forValues(kafkaSenderConfig);

        //Step 4) Create a Receiver which we'll reuse to subscribe to Records
        AloKafkaReceiver<Object, String> receiver = AloKafkaReceiver.forValues(kafkaReceiverConfig);

        //Step 5) Send some Record values to a hardcoded topic, using values as Record keys
        sender.sendValues(Flux.just("Test"), value -> TOPIC_1, Function.identity())
            .collectList()
            .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
            .block();

        //Step 6) Apply consumption of the Kafka topic we've produced data to as a stream process.
        // The "process" in this stream upper-cases the values we sent previously, producing the
        // result to another topic. This portion also adheres to the responsibilities obliged by
        // the consumption of Alo data (by acknowledging). Note that we again need to explicitly
        // limit the number of results we expect ('.take(1)'), or else this Flow would never
        // complete.
        receiver.receiveAloValues(Collections.singletonList(TOPIC_1))
            .map(String::toUpperCase)
            .transform(sender.sendAloValues(TOPIC_2, Function.identity()))
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(processedSenderResults -> System.out.println("processedSenderResults: " + processedSenderResults))
            .block();

        //Step 7) Consume the now-processed results
        receiver.receiveAloValues(Collections.singletonList(TOPIC_2))
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(downstreamResults -> System.out.println("downstreamResults: " + downstreamResults))
            .block();

        System.exit(0);
    }
}
