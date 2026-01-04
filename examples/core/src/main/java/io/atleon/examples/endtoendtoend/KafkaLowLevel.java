package io.atleon.examples.endtoendtoend;

import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverOptions;
import io.atleon.kafka.KafkaReceiverRecord;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderOptions;
import io.atleon.kafka.KafkaSenderRecord;
import io.atleon.kafka.embedded.EmbeddedKafka;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class KafkaLowLevel {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    public static void main(String[] args) throws Exception {
        // Step 1) Configure topic
        String topic = "my-topic";

        // Step 2) Specify sending options
        KafkaSenderOptions<Long, String> senderOptions = KafkaSenderOptions.<Long, String>newBuilder()
                .producerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "client-id")
                .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
                .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .build();

        // Step 3) Create Sender, and send messages periodically
        KafkaSender<Long, String> sender = KafkaSender.create(senderOptions);
        Disposable production = Flux.interval(Duration.ofMillis(500))
                .map(number -> KafkaSenderRecord.create(topic, number, "This is message #" + number, null))
                .transform(sender::send)
                .doFinally(__ -> sender.close())
                .subscribe();

        // Step 4) Specify reception options
        KafkaReceiverOptions<Long, String> receiverOptions = KafkaReceiverOptions.<Long, String>newBuilder()
                .consumerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "client-id")
                .consumerProperty(CommonClientConfigs.GROUP_ID_CONFIG, "group-id")
                .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName())
                .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .build();

        // Step 5) Create Receiver, then apply consumption
        Disposable processing = KafkaReceiver.create(receiverOptions)
                .receiveManual(Collections.singletonList(topic))
                .doOnNext(it -> System.out.println("Received message: " + it.value()))
                .subscribe(KafkaReceiverRecord::acknowledge);

        // Step 6) Wait for user to terminate, then dispose of resources (stop stream processes)
        System.in.read();
        production.dispose();
        processing.dispose();
        System.exit(0);
    }
}
