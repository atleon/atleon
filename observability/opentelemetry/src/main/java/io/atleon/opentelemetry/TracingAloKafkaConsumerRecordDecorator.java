package io.atleon.opentelemetry;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaConsumerRecordDecorator;
import io.atleon.util.ConfigLoading;
import io.opentelemetry.api.trace.SpanBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloKafkaConsumerRecordDecorator} that decorates {@link Alo} elements with tracing
 * context extracted from {@link ConsumerRecord}s
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public final class TracingAloKafkaConsumerRecordDecorator<K, V>
        extends TracingAloConsumptionDecorator<ConsumerRecord<K, V>> implements AloKafkaConsumerRecordDecorator<K, V> {

    private String clientId = null;

    private String groupId = null;

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        clientId = ConfigLoading.loadString(properties, ConsumerConfig.CLIENT_ID_CONFIG)
                .orElse(clientId);
        groupId = ConfigLoading.loadString(properties, ConsumerConfig.GROUP_ID_CONFIG)
                .orElse(groupId);
    }

    @Override
    protected SpanBuilder newSpanBuilder(SpanBuilderFactory spanBuilderFactory, ConsumerRecord<K, V> record) {
        return spanBuilderFactory
                .newSpanBuilder("atleon.receive.kafka")
                .setAttribute("client_id", Objects.toString(clientId))
                .setAttribute("group_id", Objects.toString(groupId))
                .setAttribute("topic", record.topic())
                .setAttribute("partition", record.partition())
                .setAttribute("offset", record.offset());
    }

    @Override
    protected Map<String, String> extractHeaderMap(ConsumerRecord<K, V> record) {
        Map<String, String> headerMap = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            if (header.value() != null) {
                headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        return headerMap;
    }
}
