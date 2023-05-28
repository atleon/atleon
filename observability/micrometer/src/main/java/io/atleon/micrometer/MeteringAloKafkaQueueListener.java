package io.atleon.micrometer;

import io.atleon.kafka.AloKafkaQueueListener;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link MeteringAloQueueListener} that decorates metrics with Kafka-specific
 * information (Client ID, topic, partition, etc.)
 */
public final class MeteringAloKafkaQueueListener
    extends MeteringAloQueueListener<TopicPartition>
    implements AloKafkaQueueListener {

    private String clientId = null;

    public MeteringAloKafkaQueueListener() {
        super("atleon.alo.queue.in.flight.kafka");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        clientId = ConfigLoading.loadString(properties, CommonClientConfigs.CLIENT_ID_CONFIG).orElse(clientId);
    }

    @Override
    protected TopicPartition extractKey(Object group) {
        return group instanceof TopicPartition ? TopicPartition.class.cast(group) : new TopicPartition("unknown", -1);
    }

    @Override
    protected Iterable<Tag> extractTags(TopicPartition topicPartition) {
        return Tags.of(
            Tag.of("client_id", Objects.toString(clientId)),
            Tag.of("topic", topicPartition.topic()),
            Tag.of("partition", Integer.toString(topicPartition.partition()))
        );
    }
}
