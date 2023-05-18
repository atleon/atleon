package io.atleon.schema;

/**
 * A schema-based serializer.
 *
 * @param <T> The type of data serialized by this serializer
 * @param <S> The type of schema describing serialized objects
 */
public interface SchematicSerializer<T, S> {

    default SchemaBytes<S> serialize(T data) {
        return serialize(data, (schema, outputStream) -> schema);
    }

    SchemaBytes<S> serialize(T data, SchematicPreSerializer<S> preSerializer);
}
