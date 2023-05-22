package io.atleon.schemaregistry.confluent;

import io.atleon.rabbitmq.BodyDeserializer;
import io.atleon.rabbitmq.SerializedBody;

import java.util.Map;

/**
 * A {@link BodyDeserializer} that deserializes messages as avro payloads and looks up writer
 * schemas from a Confluent schema registry.
 *
 * @param <T> Type to be deserialized into
 */
public final class AvroRegistryRabbitMQBodyDeserializer<T> implements BodyDeserializer<T> {

    private final AvroRegistryDeserializer<T> delegate = new AvroRegistryDeserializer<>();

    @Override
    public void configure(Map<String, ?> properties) {
        delegate.configure(properties);
    }

    @Override
    public T deserialize(SerializedBody data) {
        return delegate.deserialize(data.bytes());
    }
}
