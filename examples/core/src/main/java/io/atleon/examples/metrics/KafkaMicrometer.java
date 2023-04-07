package io.atleon.examples.metrics;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This example shows Atleon's integration with Micrometer. Atleon implements automatic meter
 * instrumentation for Alo items as well as receiver streams. In addition, Atleon provides a bridge
 * for native Kafka metrics to Micrometer such that metrics like lag, request rate, and throughput
 * are exported alongside Atleon metrics.
 */
public class KafkaMicrometer {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Config for Producer that backs Sender. Note we are adding a
        // Micrometer Metrics Reporter such that Kafka metrics are sent to Micrometer
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaMicrometer.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
            .with(ProducerConfig.ACKS_CONFIG, "all")
            .with(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());

        //Step 2) Create Kafka Config for Consumer that backs Receiver. Note we are adding a
        // Micrometer Metrics Reporter such that Kafka metrics are sent to Micrometer
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaMicrometer.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaMicrometer.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());

        //Step 3) Create a Meter registry which we'll add to the global Composite Registry (used by
        //default in resources configured above) and use to look up metrics later on
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);

        //Step 5) Produce values to the topic we'll process
        AloKafkaSender.<String, String>from(kafkaSenderConfig)
            .sendValues(Flux.just("test"), TOPIC_1, Function.identity())
            .subscribe();

        //Step 6) Apply stream processing to the Kafka topic we produced records to
        AloKafkaReceiver.<String>forValues(kafkaReceiverConfig)
            .receiveAloValues(Collections.singletonList(TOPIC_1))
            .map(String::toUpperCase)
            .transform(AloKafkaSender.<String, String>from(kafkaSenderConfig).sendAloValues(TOPIC_2, Function.identity()))
            .consumeAloAndGet(Alo::acknowledge)
            .publish()
            .autoConnect(0)
            .take(1)
            .collectList()
            .doOnNext(processedSenderResults -> System.out.println("processedSenderResults: " + processedSenderResults))
            .block();

        //Step 7) There are quite a few Metrics reported by Kafka, so we'll apply some filtering
        //for "interesting" Meters for the sake of brevity in this sample. You can modify or remove
        //this filtering to see the complete set of Metrics available
        List<Meter> interestingMeters = meterRegistry.getMeters().stream()
            .filter(KafkaMicrometer::doesMeterHaveInterestingId)
            .filter(KafkaMicrometer::doesMeterHaveInterestingMeasurement)
            .sorted(Comparator.comparing(meter -> meter.getId().getName()))
            .collect(Collectors.toList());

        //The following will print Meters now contained in the Registry we created above
        System.out.println("Number of (Interesting) Meters: " + interestingMeters.size());
        for (int i = 0; i < interestingMeters.size(); i++) {
            System.out.println(String.format("================ Meter #%d ================", i + 1));
            System.out.println("Meter Name: " + interestingMeters.get(i).getId().getName());
            System.out.println("Meter Type: " + interestingMeters.get(i).getId().getType());
            System.out.println("Meter Tags: " + interestingMeters.get(i).getId().getTags());
            interestingMeters.get(i).measure().forEach(measurement ->
                System.out.println("Meter Measurement: " + measurement.toString()));
        }

        System.exit(0);
    }

    private static boolean doesMeterHaveInterestingId(Meter meter) {
        return meter.getId().getName().contains("atleon") ||
            meter.getId().getName().contains(KafkaMicrometer.class.getSimpleName()) ||
            meter.getId().getName().contains("lag");
    }

    private static boolean doesMeterHaveInterestingMeasurement(Meter meter) {
        for (Measurement measurement : meter.measure()) {
            if (!Double.isNaN(measurement.getValue()) && Double.isFinite(measurement.getValue())) {
                return true;
            }
        }
        return false;
    }
}
