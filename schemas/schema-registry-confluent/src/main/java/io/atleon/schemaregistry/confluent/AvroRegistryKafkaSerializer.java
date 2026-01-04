package io.atleon.schemaregistry.confluent;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka {@link Serializer} that delegates to {@link AvroRegistrySerializer}
 *
 * @param <T> The type of data serialized by this serializer
 */
public final class AvroRegistryKafkaSerializer<T> implements Serializer<T> {

    private final AvroRegistrySerializer<T> delegate = new AvroRegistrySerializer<>();

    private boolean isKey = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs);
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return delegate.serialize(topic, isKey, data);
    }
}
