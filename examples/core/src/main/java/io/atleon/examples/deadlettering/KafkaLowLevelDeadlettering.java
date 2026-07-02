package io.atleon.examples.deadlettering;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.core.ErrorDelegator;
import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverOptions;
import io.atleon.kafka.KafkaReceiverRecord;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderOptions;
import io.atleon.kafka.KafkaSenderRecord;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.util.Collections;

public class KafkaLowLevelDeadlettering {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String MAIN_TOPIC = "MAIN";

    private static final String DEADLETTER_TOPIC = "DEADLETTER";

    public static void main(String[] args) {
        // Step 1) Create Sender to back all production of records to Kafka
        KafkaSenderOptions<String, String> kafkaSenderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-low-level-deadlettering")
                .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .build();

        // Step 2) Create Receiver through which all consumption is backed. Note that we use an
        // Auto Offset Reset of 'earliest' to ensure we receive Records produced before subscribing
        // with our new consumer group
        KafkaReceiverOptions<String, String> kafkaReceiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
                .consumerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-low-level-deadlettering")
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaLowLevelDeadlettering.class.getSimpleName())
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .maxActiveInFlight(1)
                .build();

        // Step 3) Create a Sender which we'll reuse to produce Records
        KafkaSender<String, String> sender = KafkaSender.create(kafkaSenderOptions);

        // Step 4) Send two records, one "bad" and one "good"
        Flux.just("bad", "good")
                .map(it -> KafkaSenderRecord.create(MAIN_TOPIC, "key", it, it))
                .transform(sender::send)
                .collectList()
                .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
                .block();

        // Step 5) Create a dead-lettering error delegator
        ErrorDelegator<ConsumerRecord<String, String>> deadletterDelegator =
                ErrorDelegator.<KafkaSenderRecord<String, String, ?>>sending(sender::send)
                        .composeData(it -> KafkaSenderRecord.create(DEADLETTER_TOPIC, it.key(), it.value(), it));

        // Step 6) Apply consumption of the main topic we've produced data to as a stream process.
        // When we encounter "bad" data that causes an error, the resulting error causes error
        // delegation, resulting in invocation of our custom "dead-letter" error delegator. Note
        // that error delegation requires wrapping/switching to the high level Alo API, which is
        // done by simply wrapping the initial low-level Flux.
        KafkaReceiver.create(kafkaReceiverOptions)
                .receiveManual(Collections.singletonList(MAIN_TOPIC))
                .as(AloFlux.wrapper(KafkaReceiverRecord::toAloConsumerRecord))
                .addAloErrorDelegation(deadletterDelegator)
                .map(consumerRecord -> {
                    if (consumerRecord.value().equalsIgnoreCase("bad")) {
                        throw new UnsupportedOperationException("Boom");
                    } else {
                        return consumerRecord.value().toUpperCase();
                    }
                })
                .onAloErrorDelegate()
                .consumeAloAndGet(Alo::acknowledge)
                .take(1)
                .collectList()
                .doOnNext(receivedValues -> System.out.println("receivedValues: " + receivedValues))
                .block();

        // Step 7) Receive the values that were dead-lettered
        KafkaReceiver.create(kafkaReceiverOptions)
                .receiveManual(Collections.singletonList(DEADLETTER_TOPIC))
                .map(KafkaReceiverRecord::value)
                .take(1)
                .collectList()
                .doOnNext(deadletteredValues -> System.out.println("deadletteredValues: " + deadletteredValues))
                .block();

        System.exit(0);
    }
}
