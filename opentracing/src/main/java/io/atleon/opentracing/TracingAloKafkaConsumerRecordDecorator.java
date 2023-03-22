package io.atleon.opentracing;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaConsumerRecordDecorator;
import io.atleon.util.ConfigLoading;
import io.opentracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An {@link AloKafkaConsumerRecordDecorator} that decorates {@link Alo} elements with tracing
 * context extracted from {@link ConsumerRecord}s
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public class TracingAloKafkaConsumerRecordDecorator<K, V>
    extends TracingAloConsumptionDecorator<ConsumerRecord<K, V>>
    implements AloKafkaConsumerRecordDecorator<K, V> {

    private String groupId = null;

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        groupId = ConfigLoading.loadString(properties, ConsumerConfig.GROUP_ID_CONFIG).orElse(groupId);
    }

    @Override
    protected Tracer.SpanBuilder newSpanBuilder(SpanBuilderFactory spanBuilderFactory, ConsumerRecord<K, V> record) {
        return spanBuilderFactory.newSpanBuilder("atleon.kafka.consume")
            .withTag("group", groupId)
            .withTag("topic", record.topic())
            .withTag("partition", record.partition())
            .withTag("offset", record.offset());
    }

    @Override
    protected Map<String, String> extractHeaderMap(ConsumerRecord<K, V> record) {
        Map<String, String> headerMap = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headerMap;
    }
}
