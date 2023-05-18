package io.atleon.schema;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * A schema that may have an identifying key associated with it. This is useful when the provided
 * schema describes one that was used to serialize data, and deducing or generating an appropriate
 * schema to deserialize with is based on that written schema. In such a case, that "reader" schema
 * can be memoized based on the "key" associated with the "writer" schema.
 *
 * @param <S> The type of schema that may have a key associated with it
 */
public final class KeyableSchema<S> {

    private final Serializable key;

    private final S schema;

    private KeyableSchema(Serializable key, S schema) {
        this.key = key;
        this.schema = Objects.requireNonNull(schema);
    }

    public static <S> KeyableSchema<S> keyed(Serializable key, S schema) {
        return new KeyableSchema<>(Objects.requireNonNull(key), schema);
    }

    public static <S> KeyableSchema<S> unkeyed(S schema) {
        return new KeyableSchema<>(null, schema);
    }

    public Optional<Serializable> key() {
        return Optional.ofNullable(key);
    }

    public S schema() {
        return schema;
    }
}
