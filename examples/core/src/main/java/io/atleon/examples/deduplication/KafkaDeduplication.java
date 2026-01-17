package io.atleon.examples.deduplication;

import io.atleon.core.Alo;
import io.atleon.core.Deduplication;
import io.atleon.core.DeduplicationConfig;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.KafkaSenderResult;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * This example shows how message consumption may be de-duped via
 * {@link io.atleon.core.AloFlux#deduplicate(Deduplication, DeduplicationConfig)}.
 * Note that this example continues to incorporate {@link io.atleon.core.Alo} acknowledgement
 * propagation such that at-least-once guarantee is maintained in the face of "aggregation", or
 * "many to one" processing transformations.
 */
public class KafkaDeduplication {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) throws Exception {
        // Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaDeduplication.class.getSimpleName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) Create Kafka Config for Consumer that backs Receiver. Note that set the auto
        // offset reset to earliest such that subsequently produced Records are processed
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaDeduplication.class.getSimpleName())
                .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaDeduplication.class.getSimpleName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Step 3) Create a deduplication config where, upon the first occurrence of an item, a
        // window with a maximum timespan of 2 seconds and a max size of 4 items is opened
        DeduplicationConfig deduplicationConfig = new DeduplicationConfig(Duration.ofSeconds(2), 4L);

        // Step 4) Apply consumption of the Kafka topic we've produced data to as a stream process.
        // The "process" in this stream upper-cases the values we sent previously, producing the
        // result to another topic. This portion also adheres to the responsibilities obliged by
        // the consumption of Alo data (by acknowledging). Note that we again need to explicitly
        // limit the time for which we take items (`take(Duration)`), or else this Flow would
        // never complete. Deduplication is applied early in the stream to limit the processing of
        // duplicate methods via AloFlux::deduplicate
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);
        Mono<List<String>> processed = AloKafkaReceiver.<Object, String>create(kafkaReceiverConfig)
                .receiveAloValues(TOPIC_1)
                .deduplicate(Deduplication.identity(), deduplicationConfig)
                .map(String::toUpperCase)
                .transform(sender.sendAloValues(TOPIC_2, Function.identity()))
                .consumeAloAndGet(Alo::acknowledge)
                .map(KafkaSenderResult::correlationMetadata)
                .doOnNext(next -> System.out.println("Processed next=" + next))
                .switchOnFirst((signal, flux) -> signal.hasValue() ? flux.take(Duration.ofSeconds(5)) : flux)
                .cache()
                .publish()
                .autoConnect(0)
                .collectList();

        // Step 5) Create the List of values we'll emit to the topic. These values will be
        // immediately emitted such that the above stream process will see a near immediate
        // consumption of the following counts of data:
        // "ONE" - 2 times
        // "TWO" - 6 times
        // "THREE" - 1 time
        // Due to our deduplication config, we should see two processing occurrences of "TWO" where
        // the first executes near-immediately, followed by the second that is processed around
        // two seconds later with singular occurrences of "ONE" and "THREE"
        List<String> values = Arrays.asList("TWO", "TWO", "ONE", "TWO", "THREE", "TWO", "ONE", "TWO", "TWO");

        // Step 6) Send the above values to the Kafka topic we're processing
        sender.sendValues(Flux.fromIterable(values), TOPIC_1, Function.identity())
                .subscribe();

        // Step 7) Await consumption of the results
        List<String> result = processed.block();
        System.out.println("result=" + result);

        System.exit(0);
    }
}
