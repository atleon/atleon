package io.atleon.examples.endtoendtoend;

import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverOptions;
import io.atleon.kafka.KafkaReceiverRecord;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderOptions;
import io.atleon.kafka.KafkaSenderRecord;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;

public class KafkaLowLevel {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    public static void main(String[] args) throws Exception {
        //Step 1) Configure topic
        String topic = "my-topic";

        //Step 2) Periodically produce messages asynchronously
        Disposable production = periodicallyProduceMessages(BOOTSTRAP_SERVERS, topic, Duration.ofMillis(100));

        //Step 3) Specify reception options
        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "client-id")
            .consumerProperty(CommonClientConfigs.GROUP_ID_CONFIG, "group-id")
            .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .build();

        //Step 4) Create Receiver, then apply consumption
        Disposable processing = KafkaReceiver.create(options)
            .receiveManual(Collections.singletonList(topic))
            .doOnNext(record -> System.out.println("Received message: " + record.value()))
            .doOnNext(KafkaReceiverRecord::acknowledge)
            .subscribe();

        //Step 5) Wait for user to terminate, then dispose of resources (stop stream processes)
        System.in.read();
        production.dispose();
        processing.dispose();
    }

    private static Disposable periodicallyProduceMessages(String bootstrapServers, String topic, Duration period) {
        //Step 1) Specify sending options
        KafkaSenderOptions<String, String> options = KafkaSenderOptions.<String, String>newBuilder()
            .producerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "client-id")
            .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .build();

        //Step 2) Create Sender, and messages periodically
        KafkaSender<String, String> sender = KafkaSender.create(options);
        return Flux.interval(period)
            .map(number -> new ProducerRecord<>(topic, number.toString(), "This is message #" + (number + 1)))
            .map(producerRecord -> KafkaSenderRecord.create(producerRecord, null))
            .transform(sender::send)
            .doFinally(__ -> sender.close())
            .subscribe();
    }
}
