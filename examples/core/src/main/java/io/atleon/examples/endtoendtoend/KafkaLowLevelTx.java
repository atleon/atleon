package io.atleon.examples.endtoendtoend;

import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverOptions;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderOptions;
import io.atleon.kafka.KafkaSenderRecord;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;

public class KafkaLowLevelTx {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    public static void main(String[] args) throws Exception {
        // Step 1) Configure topics
        String inputTopic = "input-topic";
        String outputTopic = "output-topic";

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
                .map(number -> KafkaSenderRecord.create(inputTopic, number, "This is message #" + number, null))
                .transform(sender::send)
                .doFinally(__ -> sender.close())
                .subscribe();

        // Step 4) Specify transactional sending options
        KafkaSenderOptions<Long, String> txSenderOptions = KafkaSenderOptions.<Long, String>newBuilder()
                .producerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "client-id")
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id")
                .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
                .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .build();

        // Step 5) Specify reception options
        KafkaReceiverOptions<Long, String> receiverOptions = KafkaReceiverOptions.<Long, String>newBuilder()
                .consumerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "client-id")
                .consumerProperty(CommonClientConfigs.GROUP_ID_CONFIG, "group-id")
                .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName())
                .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .build();

        // Step 6) Create Receiver, then apply consumption
        KafkaSender<Long, String> txSender = KafkaSender.create(txSenderOptions);
        Disposable processing = KafkaReceiver.create(receiverOptions)
                .receiveTxManual(txSender.txManager(), Collections.singletonList(inputTopic))
                .map(it -> KafkaSenderRecord.create(
                        outputTopic, it.key(), it.value().toUpperCase(), it))
                .transform(txSender::send)
                .doOnNext(result -> System.out.println("Processed message: " + result.recordMetadata()))
                .doOnNext(result -> result.correlationMetadata().acknowledge())
                .doFinally(__ -> txSender.close())
                .subscribe();

        // Step 7) Wait for user to terminate, then dispose of resources (stop stream processes)
        System.in.read();
        production.dispose();
        processing.dispose();
        System.exit(0);
    }
}
