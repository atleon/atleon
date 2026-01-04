package io.atleon.micrometer;

import io.atleon.kafka.AloKafkaConsumerRecordSignalListenerFactory;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An {@link AloKafkaConsumerRecordSignalListenerFactory} that creates
 * {@link reactor.core.observability.SignalListener} instances which apply metering to Reactor
 * Publishers of {@link io.atleon.core.Alo} items referencing Kafka {@link ConsumerRecord}.
 *
 * @param <K> The types of keys in records consumed by this factory's listeners
 * @param <V> The types of values in records consumed by this factory's listeners
 */
public final class MeteringAloKafkaConsumerRecordSignalListenerFactory<K, V>
        extends MeteringAloSignalListenerFactory<ConsumerRecord<K, V>, String>
        implements AloKafkaConsumerRecordSignalListenerFactory<K, V, Void> {

    private String clientId = null;

    public MeteringAloKafkaConsumerRecordSignalListenerFactory() {
        super("atleon.alo.publisher.signal.receive.kafka");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        clientId = ConfigLoading.loadString(properties, CommonClientConfigs.CLIENT_ID_CONFIG)
                .orElse(clientId);
    }

    @Override
    protected Function<? super ConsumerRecord<K, V>, String> keyExtractor() {
        return ConsumerRecord::topic;
    }

    @Override
    protected Tagger<? super String> tagger() {
        return Tagger.composed(Tags.of("client_id", Objects.toString(clientId)), topic -> Tags.of("topic", topic));
    }
}
