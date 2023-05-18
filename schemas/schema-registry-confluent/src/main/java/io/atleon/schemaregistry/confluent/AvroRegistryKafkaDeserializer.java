package io.atleon.schemaregistry.confluent;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * A Kafka {@link Deserializer} that delegates to {@link AvroRegistryDeserializer}
 *
 * @param <T> The type of data deserialized by this deserializer
 */
public final class AvroRegistryKafkaDeserializer<T> implements Deserializer<T> {

    private final AvroRegistryDeserializer<T> delegate = new AvroRegistryDeserializer<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return delegate.deserialize(data);
    }
}
