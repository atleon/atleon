package io.atleon.examples.errorhandling;

import io.atleon.core.DefaultAloSenderResultSubscriber;
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
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * This example demonstrates how to apply resiliency characteristics to Kafka streaming processes
 * via "resubscription". When either an upstream error or downstream error occurs on an Alo, an
 * error will be emitted in the Flux produced by the Kafka Receiver. This causes the
 * following sequence of events:
 * 1) Cancellation of the first Flux of values from the Kafka Receiver
 * 2) Commitment of acknowledged offsets
 * 3) Closure of the underlying Kafka Consumer
 * 4) Resubscription, resulting in a new Kafka Consumer and new Flux
 * 5) Reconsumption of records whose offsets have not yet been committed
 * This example also illustrates how we ensure that any given Record's offset is not committed
 * until after all records in the same topic-partition that come before it have been acknowledged.
 * This is a core behavior of Atleon's at-least-once guarantee
 */
public class KafkaErrorHandling {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaErrorHandling.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2) Create "faulty" Kafka Config where the second value we try to process will throw
        // an Exception at serialization time
        KafkaConfigSource faultyKafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaErrorHandling.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SecondTimeFailingSerializer.class.getName());

        //Step 3) Create Kafka Config for Consumer that backs Receiver
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaErrorHandling.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaErrorHandling.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 4) Apply stream processing to the Kafka topic we'll produce records to. The
        // "processing" in this case is contrived to block a particular record, "TEST_1", from
        // proceeding while a record that comes strictly after it, "TEST_2", has finished
        // processing. At that point, "TEST_1" will fail due to our "faulty" configuration where
        // serialization will fail the second time it's executed. This results in a "negative
        // acknowledgement" which causes an upstream error to be emitted. Coupled with the
        // "resubscription" we have added to this process, the end result is that we re-consume
        // BOTH records and successfully process them
        CountDownLatch latch = new CountDownLatch(1);
        List<String> successfullyProcessed = new CopyOnWriteArrayList<>();
        AloKafkaSender<String, String> faultySender = AloKafkaSender.create(faultyKafkaSenderConfig);
        AloKafkaReceiver.<Object, String>create(kafkaReceiverConfig)
            .receiveAloValues(TOPIC_1)
            .groupBy(Function.identity(), Integer.MAX_VALUE)
            .innerPublishOn(Schedulers.boundedElastic())
            .innerMap(String::toUpperCase)
            .innerDoOnNext(next -> {
                try {
                    if (next.equals("TEST_1")) {
                        latch.await();
                    }
                } catch (Exception e) {
                    System.err.println("Unexpected failure=" + e);
                }
            })
            .flatMapAlo(faultySender.sendAloValues(TOPIC_2, Function.identity()))
            .doOnNext(next -> latch.countDown())
            .doOnNext(senderResult -> {
                if (!senderResult.isFailure()) {
                    successfullyProcessed.add(senderResult.correlationMetadata());
                }
            })
            .resubscribeOnError(KafkaErrorHandling.class.getSimpleName(), Duration.ofSeconds(2L))
            .doFinally(faultySender::close)
            .subscribe(new DefaultAloSenderResultSubscriber<>());

        //Step 5) Produce the records to be consumed above. Note that we are using the same record
        // key for each data item, which will cause these items to show up in the order we emit
        // them on the same topic-partition
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);
        Flux.just("test_1", "test_2")
            .subscribeOn(Schedulers.boundedElastic())
            .transform(sender.sendValues(TOPIC_1, string -> "KEY"))
            .doFinally(sender::close)
            .subscribe();

        //Step 6) Await the successful completion of the data we emitted. There should be exactly
        // three successfully processed elements, since we end up successfully processing "test_2"
        // twice
        while (successfullyProcessed.size() < 3) {
            Thread.sleep(100L);
        }

        System.out.println("processed: " + successfullyProcessed);
        System.exit(0);
    }

    public static class SecondTimeFailingSerializer extends StringSerializer {

        private static final AtomicInteger COUNT = new AtomicInteger();

        @Override
        public byte[] serialize(String topic, String data) {
            if (COUNT.incrementAndGet() == 2) {
                throw new IllegalStateException("This serializer will fail the second time it's used");
            }
            return super.serialize(topic, data);
        }
    }
}
