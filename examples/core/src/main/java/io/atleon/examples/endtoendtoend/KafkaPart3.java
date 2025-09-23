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
 * Part 3 of this example set extends the consumption of a Kafka topic to a "stream" process. This
 * also introduces the concept of Acknowledgement propagation, where the Records we've received
 * are not acknowledged (i.e. have their offsets marked ready for commit) until the full downstream
 * transformation has successfully completed.
 */
public class KafkaPart3 {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = KafkaPart3.class.getSimpleName() + "-1";

    private static final String TOPIC_2 = KafkaPart3.class.getSimpleName() + "-2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart3.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        //Step 2) Create Kafka Config for Consumer that backs Receiver. Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing with
        // our new consumer group
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart3.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaPart3.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Create a Sender which we'll reuse to produce Records
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);

        //Step 4) Send some Record values to a hardcoded topic, using values as Record keys
        sender.sendValues(Flux.just("Test"), TOPIC_1, Function.identity())
            .collectList()
            .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
            .block();

        //Step 5) Apply consumption of the Kafka topic we've produced data to as a stream process.
        // The "process" in this stream upper-cases the values we sent previously, producing the
        // result to another topic. This portion also adheres to the responsibilities obliged by
        // the consumption of Alo data (by acknowledging). Note that we again need to explicitly
        // limit the number of results we expect ('.take(1)'), or else this Flow would never
        // complete.
        AloKafkaReceiver.<Object, String>create(kafkaReceiverConfig)
            .receiveAloValues(TOPIC_1)
            .map(String::toUpperCase)
            .transform(sender.sendAloValues(TOPIC_2, Function.identity()))
            .consumeAloAndGet(Alo::acknowledge)
            .take(1)
            .collectList()
            .doOnNext(processedSenderResults -> System.out.println("processedSenderResults: " + processedSenderResults))
            .block();

        System.exit(0);
    }
}
