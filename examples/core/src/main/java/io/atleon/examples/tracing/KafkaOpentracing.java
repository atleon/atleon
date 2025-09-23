package io.atleon.examples.tracing;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
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
 * This example shows Atleon's integration with Opentracing. Due to the inclusion of
 * atleon-opentracing-auto as a runtime dependency, Alo items are automatically decorated with
 * tracing context which is propagated through the pipeline.
 */
public class KafkaOpentracing {

    private static final String BOOTSTRAP_SERVERS = EmbeddedKafka.startAndGetBootstrapServersConnect();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) {
        //Step 1) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaOpentracing.class.getSimpleName())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2) Create Kafka Config for Consumer that backs Receiver
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaOpentracing.class.getSimpleName())
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaOpentracing.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Install a Tracer to the GlobalTracer registry. Note that tracing is automatically
        // activated due to a dependency on atleon-opentracing-auto
        MockTracer tracer = new MockTracer();
        GlobalTracer.registerIfAbsent(tracer);

        //Step 4) Create an AloKafkaSender with which we'll send records
        AloKafkaSender<String, String> sender = AloKafkaSender.create(kafkaSenderConfig);

        //Step 5) Produce records to our input topic
        int numToEmit = 4;
        Flux.range(1, numToEmit)
            .map(index -> "test-" + index)
            .transform(sender.sendValues(TOPIC_1, Function.identity()))
            .then().block();

        //Step 6) Process the records we produced to another topic
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

        //Before printing, sort the finished Spans by start time (then ID) such that they are
        //easier to read below
        List<MockSpan> sortedSpans = tracer.finishedSpans().stream()
            .sorted(Comparator.comparing(MockSpan::startMicros).thenComparing(mockSpan -> mockSpan.context().spanId()))
            .collect(Collectors.toList());

        //This will print out all the spans
        System.out.println("Number of Spans: " + sortedSpans.size());
        for (int i = 0; i < sortedSpans.size(); i++) {
            System.out.printf("================ Span #%d ================%n", i + 1);
            System.out.println("Trace ID: " + sortedSpans.get(i).context().traceId());
            System.out.println("Span ID: " + sortedSpans.get(i).context().spanId());
            System.out.println("Parentage: " + extractParentage(sortedSpans.get(i)));
            System.out.println("Operation Name: " + sortedSpans.get(i).operationName());

            List<MockSpan.LogEntry> logEntries = sortedSpans.get(i).logEntries();
            System.out.println("Number of Log Entries: " + logEntries.size());
            for (int j = 0; j < logEntries.size(); j++) {
                MockSpan.LogEntry entry = logEntries.get(j);
                System.out.printf("Log Entry #%d: timestamp=%d fields=%s%n", j, entry.timestampMicros(), entry.fields());
            }
        }

        System.exit(0);
    }

    private static String extractParentage(MockSpan mockSpan) {
        if (mockSpan.references().isEmpty()) {
            return "Root Span";
        } else {
            return mockSpan.references().stream()
                .map(reference -> "{type:" + reference.getReferenceType() + ", id:" + reference.getContext().spanId() + "}")
                .collect(Collectors.joining(", ", "[", "]"));
        }
    }
}
