package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaConsumerRecordDecorator;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloKafkaConsumerRecordDecorator} that decorates {@link Alo} elements with metering
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public class MeteringAloKafkaConsumerRecordDecorator<K, V>
    extends MeteringAloDecorator<ConsumerRecord<K, V>>
    implements AloKafkaConsumerRecordDecorator<K, V> {

    private String clientId = null;

    public MeteringAloKafkaConsumerRecordDecorator() {
        super("atleon.alo.kafka.receive");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        clientId = ConfigLoading.loadString(properties, CommonClientConfigs.CLIENT_ID_CONFIG).orElse(clientId);
    }

    @Override
    protected Tags extractTags(ConsumerRecord<K, V> consumerRecord) {
        return Tags.of(
            Tag.of("client", Objects.toString(clientId)),
            Tag.of("topic", consumerRecord.topic())
        );
    }
}
