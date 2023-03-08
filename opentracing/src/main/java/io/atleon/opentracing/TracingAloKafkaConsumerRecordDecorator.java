package io.atleon.opentracing;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaConsumerRecordDecorator;
import io.atleon.util.ConfigLoading;
import io.opentracing.References;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * An {@link AloKafkaConsumerRecordDecorator} that decorates {@link Alo} elements with tracing
 * context extracted from {@link ConsumerRecord}s
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public class TracingAloKafkaConsumerRecordDecorator<K, V> extends ConsumerTracing implements AloKafkaConsumerRecordDecorator<K, V> {

    private String groupId = null;

    @Override
    public void configure(Map<String, ?> properties) {
        groupId = ConfigLoading.load(properties, ConsumerConfig.GROUP_ID_CONFIG, Function.identity(), groupId);
    }

    @Override
    public Alo<ConsumerRecord<K, V>> decorate(Alo<ConsumerRecord<K, V>> alo) {
        ConsumerRecord<K, V> consumerRecord = alo.get();
        Tracer.SpanBuilder spanBuilder = newSpanBuilder("atleon.kafka.consume")
            .withTag("group", groupId)
            .withTag("topic", consumerRecord.topic())
            .withTag("partition", consumerRecord.partition())
            .withTag("offset", consumerRecord.offset());
        extractSpanContext(consumerRecord)
            .ifPresent(it -> spanBuilder.addReference(References.FOLLOWS_FROM, it));
        return TracingAlo.start(alo, tracerFacade(), spanBuilder);
    }

    protected Optional<SpanContext> extractSpanContext(ConsumerRecord<?, ?> consumerRecord) {
        Map<String, String> headerMap = new LinkedHashMap<>();
        for (Header header : consumerRecord.headers()) {
            headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return extractSpanContext(headerMap);
    }
}
