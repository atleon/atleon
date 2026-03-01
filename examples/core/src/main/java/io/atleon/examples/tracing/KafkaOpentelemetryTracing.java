package io.atleon.examples.tracing;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This example shows Atleon's tracing integration with Opentelemetry. Due to the inclusion of
 * atleon-opentelemetry-tracing-auto as a runtime dependency, Alo items are automatically decorated
 * with tracing context which is propagated through the pipeline.
 */
public class KafkaOpentelemetryTracing {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) {
        // Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaOpentelemetryTracing.class.getSimpleName())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2) Create Kafka Config for Consumer that backs Receiver
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
                .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaOpentelemetryTracing.class.getSimpleName())
                .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaOpentelemetryTracing.class.getSimpleName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Step 3) Install a TracerProvider with in-memory SpanExporter to the global OpenTelemetry
        // registry. Note that tracing is automatically activated due to a dependency on
        // atleon-opentelemetry-tracing-auto
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();

        // Step 4) Create an AloKafkaSender with which we'll send records
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);

        // Step 5) Produce records to our input topic
        int numToEmit = 4;
        Flux.range(1, numToEmit)
                .map(index -> "test-" + index)
                .transform(sender.sendValues(TOPIC_1, Function.identity()))
                .then()
                .block();

        // Step 6) Process the records we produced to another topic
        int numToCombine = 2;
        AloKafkaReceiver.<String, String>create(kafkaReceiverConfig)
                .receiveAloValues(TOPIC_1)
                .bufferTimeout(numToCombine, Duration.ofNanos(Long.MAX_VALUE))
                .map(strings -> String.join(" ", strings))
                .transform(sender.sendAloValues(TOPIC_2, Function.identity()))
                .consumeAloAndGet(Alo::acknowledge)
                .take(numToEmit / numToCombine)
                .collectList()
                .doOnNext(results -> System.out.println("Sender Results: " + results))
                .block();

        // Before printing, sort the finished Spans by start time (then ID) such that they are
        // easier to read below
        List<SpanData> sortedSpans = spanExporter.getFinishedSpanItems().stream()
                .sorted(Comparator.comparing(SpanData::getStartEpochNanos).thenComparing(SpanData::getSpanId))
                .collect(Collectors.toList());

        // This will print out all the spans
        System.out.println("Number of Spans: " + sortedSpans.size());
        for (int i = 0; i < sortedSpans.size(); i++) {
            System.out.printf("================ Span #%d ================%n", i + 1);
            System.out.println("Trace ID: " + sortedSpans.get(i).getTraceId());
            System.out.println("Span ID: " + sortedSpans.get(i).getSpanId());
            System.out.println("Parentage: " + extractParentage(sortedSpans.get(i)));
            System.out.println("Operation Name: " + sortedSpans.get(i).getName());
            System.out.println("Attributes: " + sortedSpans.get(i).getAttributes());

            List<EventData> eventEntries = sortedSpans.get(i).getEvents();
            System.out.println("Number of Event Entries: " + eventEntries.size());
            for (int j = 0; j < eventEntries.size(); j++) {
                EventData entry = eventEntries.get(j);
                System.out.printf(
                        "Event #%d: timestamp=%d attributes=%s%n", j, entry.getEpochNanos(), entry.getAttributes());
            }
        }

        System.exit(0);
    }

    private static String extractParentage(SpanData spanData) {
        if (spanData.getLinks().isEmpty()) {
            return "Root Span";
        } else {
            return spanData.getLinks().stream()
                    .map(it -> String.format("{context: %s, attributes: %s}", it.getSpanContext(), it.getAttributes()))
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }
}
