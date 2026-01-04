package io.atleon.examples.endtoendtoend;

import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

/**
 * Part 1 of this example set demonstrates how to send our first Records to an (embedded) Kafka
 * cluster.
 */
public class KafkaPart1 {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC = KafkaPart1.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        // Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
                .with(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(ProducerConfig.CLIENT_ID_CONFIG, KafkaPart1.class.getSimpleName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) Send some Record values to a hardcoded topic, using values as Record keys
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);
        sender.sendValues(Flux.just("Test"), TOPIC, Function.identity())
                .collectList()
                .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
                .doFinally(sender::close)
                .block();

        System.exit(0);
    }
}
