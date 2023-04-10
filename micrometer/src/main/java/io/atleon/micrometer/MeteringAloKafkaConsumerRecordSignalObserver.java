package io.atleon.micrometer;

import io.atleon.kafka.AloKafkaConsumerRecordSignalObserver;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloKafkaConsumerRecordSignalObserver} that applies metering to emitted
 * {@link reactor.core.publisher.Signal}s referencing {@link io.atleon.core.Alo} of Kafka
 * {@link ConsumerRecord}.
 *
 * @param <K> The types of keys in records consumed by this observer
 * @param <V> The types of values in records consumed by this observer
 */
public final class MeteringAloKafkaConsumerRecordSignalObserver<K, V>
    extends MeteringAloSignalObserver<ConsumerRecord<K, V>, String>
    implements AloKafkaConsumerRecordSignalObserver<K, V> {

    private String clientId = null;

    public MeteringAloKafkaConsumerRecordSignalObserver() {
        super("atleon.alo.publisher.signal.receive.kafka");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        clientId = ConfigLoading.loadString(properties, CommonClientConfigs.CLIENT_ID_CONFIG).orElse(clientId);
    }

    @Override
    protected String extractKey(ConsumerRecord<K, V> consumerRecord) {
        return consumerRecord.topic();
    }

    @Override
    protected Iterable<Tag> baseTags() {
        return Tags.of("client_id", Objects.toString(clientId));
    }

    @Override
    protected Iterable<Tag> extractTags(String topic) {
        return Tags.of("topic", topic);
    }
}
