package io.atleon.examples.deadlettering;

import io.atleon.core.Alo;
import io.atleon.core.ErrorDelegator;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

public class KafkaDeadlettering {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String MAIN_TOPIC = "MAIN";

    private static final String DEADLETTER_TOPIC = "DEADLETTER";

    public static void main(String[] args) {
        // Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaDeadlettering.class.getSimpleName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) Create Kafka Config for Consumer that backs Receiver. Note that we use an Auto
        // Offset Reset of 'earliest' to ensure we receive Records produced before subscribing with
        // our new consumer group
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaDeadlettering.class.getSimpleName())
                .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaDeadlettering.class.getSimpleName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(AloKafkaReceiver.MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG, 1);

        // Step 3) Create a Sender which we'll reuse to produce Records
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);

        // Step 4) Send two records, one "bad" and one "good"
        sender.sendValues(Flux.just("bad", "good"), MAIN_TOPIC, __ -> "key")
                .collectList()
                .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
                .block();

        // Step 5) Create a dead-lettering error delegator
        ErrorDelegator<String> deadletterDelegator = ErrorDelegator.sending(sender::sendRecord)
                .composeData(string -> new ProducerRecord<>(DEADLETTER_TOPIC, "key", string));

        // Step 6) Apply consumption of the main topic we've produced data to as a stream process.
        // When we encounter "bad" data that causes an error, we will "dead-letter" it using the
        // previously-defined delegator. We also use onAloErrorDelegate to activate error delegation
        AloKafkaReceiver.<Object, String>create(kafkaReceiverConfig)
                .receiveAloValues(MAIN_TOPIC)
                .addAloErrorDelegation(deadletterDelegator)
                .map(string -> {
                    if (string.equalsIgnoreCase("bad")) {
                        throw new UnsupportedOperationException("Boom");
                    } else {
                        return string.toUpperCase();
                    }
                })
                .onAloErrorDelegate()
                .consumeAloAndGet(Alo::acknowledge)
                .take(1)
                .collectList()
                .doOnNext(receivedValues -> System.out.println("receivedValues: " + receivedValues))
                .block();

        // Step 7) Receive the values that were dead-lettered
        AloKafkaReceiver.<Object, String>create(kafkaReceiverConfig)
                .receiveAloValues(DEADLETTER_TOPIC)
                .consumeAloAndGet(Alo::acknowledge)
                .take(1)
                .collectList()
                .doOnNext(deadletteredValues -> System.out.println("deadletteredValues: " + deadletteredValues))
                .block();

        System.exit(0);
    }
}
