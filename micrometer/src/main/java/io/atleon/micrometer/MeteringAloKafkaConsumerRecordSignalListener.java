package io.atleon.micrometer;

import io.atleon.kafka.AloKafkaConsumerRecordSignalListener;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloKafkaConsumerRecordSignalListener} that applies metering to emitted
 * {@link reactor.core.publisher.Signal}s referencing {@link io.atleon.core.Alo} of Kafka
 * {@link ConsumerRecord}.
 *
 * @param <K> The types of keys in records consumed by this listener
 * @param <V> The types of values in records consumed by this listener
 */
public class MeteringAloKafkaConsumerRecordSignalListener<K, V>
    extends MeteringAloSignalListener<ConsumerRecord<K, V>>
    implements AloKafkaConsumerRecordSignalListener<K, V> {

    private String clientId = null;

    public MeteringAloKafkaConsumerRecordSignalListener() {
        super("atleon.alo.publisher.signal.kafka.receive");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        clientId = ConfigLoading.loadString(properties, CommonClientConfigs.CLIENT_ID_CONFIG).orElse(clientId);
    }

    @Override
    protected Iterable<Tag> baseTags() {
        return Collections.singleton(Tag.of("client", Objects.toString(clientId)));
    }
}
