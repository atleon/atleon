package io.atleon.schema;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * A schema-based deserializer.
 *
 * @param <T> The type of data deserialized by this deserializer
 * @param <S> The type of schema describing deserialized objects
 */
public interface SchematicDeserializer<T, S> {

    default T deserialize(byte[] data, S writerSchema) {
        return deserialize(data, dataBuffer -> KeyableSchema.unkeyed(writerSchema));
    }

    default T deserialize(byte[] data, KeyableSchema<S> keyableWriterSchema) {
        return deserialize(data, dataBuffer -> keyableWriterSchema);
    }

    T deserialize(byte[] data, Function<ByteBuffer, KeyableSchema<S>> dataBufferToWriterSchema);
}
