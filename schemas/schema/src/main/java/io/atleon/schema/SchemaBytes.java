package io.atleon.schema;

import java.util.Objects;

/**
 * A schema and data that has been serialized according to that schema.
 *
 * @param <S> The type of schema describing serialized bytes
 */
public final class SchemaBytes<S> {

    private final S schema;

    private final byte[] bytes;

    private SchemaBytes(S schema, byte[] bytes) {
        this.schema = Objects.requireNonNull(schema);
        this.bytes = Objects.requireNonNull(bytes);
    }

    public static <S> SchemaBytes<S> serialized(S schema, byte[] bytes) {
        return new SchemaBytes<>(schema, bytes);
    }

    public S schema() {
        return schema;
    }

    public byte[] bytes() {
        return bytes;
    }
}
