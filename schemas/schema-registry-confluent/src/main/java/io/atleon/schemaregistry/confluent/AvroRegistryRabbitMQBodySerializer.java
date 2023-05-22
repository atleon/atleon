package io.atleon.schemaregistry.confluent;

import io.atleon.rabbitmq.BodySerializer;
import io.atleon.rabbitmq.SerializedBody;

import java.util.Map;

/**
 * A {@link BodySerializer} that serializes data as Avro payloads and registers (or gets) the
 * writer schema from a Confluent schema registry. This implementation causes the registered
 * subject names to be that of the generated schema name. This can be customized by creating a
 * custom implementation that has a more sophisticated subject naming mechanism, and using a
 * method on the delegate tha accepts the subject name.
 *
 * @param <T> Type to be serialized from
 */
public final class AvroRegistryRabbitMQBodySerializer<T> implements BodySerializer<T> {

    private final AvroRegistrySerializer<T> delegate = new AvroRegistrySerializer<>();

    @Override
    public void configure(Map<String, ?> properties) {
        delegate.configure(properties);
    }

    @Override
    public SerializedBody serialize(T data) {
        return SerializedBody.ofBytes(delegate.serialize(data));
    }
}
