package io.atleon.examples.parallelism;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * This example shows how to process Kafka records with arbitrarily high parallelism. You can try
 * changing the number of samples and/or number of processing groups to see significant changes
 * in processing speed. Note that in-order offset acknowledgement is handled (under the hood) by
 * {@link io.atleon.core.AloQueueingTransformer} such that offset commits are not executed past any
 * record whose offset we have not yet fully processed (acknowledged).
 */
public class KafkaArbitraryParallelism {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC = "TOPIC";

    private static final int NUM_SAMPLES = 10000;

    private static final int NUM_GROUPS = 16;

    private static final long MAX_SLEEP_MILLIS = 10;

    public static void main(String[] args) throws Exception {
        // Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaArbitraryParallelism.class.getSimpleName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) Create Kafka Config for Consumer that backs Receiver. Note that set the auto
        // offset reset to earliest such that subsequently produced Records are processed
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaArbitraryParallelism.class.getSimpleName())
                .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaArbitraryParallelism.class.getSimpleName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Step 3) Apply stream processing to the Kafka topic we'll produce records to. The
        // "processing" in this case introduces a superficial blocking sleep which might mimic an
        // IO-bound process.
        CountDownLatch latch = new CountDownLatch(NUM_SAMPLES);
        AloKafkaReceiver.<String, String>create(kafkaReceiverConfig)
                .receiveAloRecords(TOPIC)
                .groupByStringHash(ConsumerRecord::key, NUM_GROUPS, ConsumerRecord::value)
                .innerPublishOn(Schedulers.boundedElastic())
                .innerMap(String::toUpperCase)
                .innerDoOnNext(next -> {
                    try {
                        Double sleepMillis = Math.random() * MAX_SLEEP_MILLIS + 1;
                        System.out.println(String.format(
                                "next=%s thread=%s sleepMillis=%d",
                                next, Thread.currentThread().getName(), sleepMillis.longValue()));
                        Thread.sleep(sleepMillis.longValue());
                    } catch (Exception e) {
                        System.err.println("Failed to sleep");
                    }
                })
                .flatMapAlo()
                .consumeAloAndGet(Alo::acknowledge)
                .subscribe(string -> latch.countDown());

        // Step 4) Produce random UUIDs to the topic we're processing above
        Flux.range(0, NUM_SAMPLES)
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> UUID.randomUUID())
                .map(UUID::toString)
                .transform(
                        AloKafkaSender.<String, String>create(kafkaSenderConfig).sendValues(TOPIC, Function.identity()))
                .subscribe();

        // Step 5) Await processing completion of the UUIDs we produced
        Instant begin = Instant.now();
        latch.await();

        System.out.println("Processing duration=" + Duration.between(begin, Instant.now()));
        System.exit(0);
    }
}
