package io.atleon.micrometer;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Tuple of a an {@link Enum} type and arbitrary key.
 *
 * @param <T> The enumerated type of key
 * @param <K> Arbitrary identifier type
 */
final class TypeKey<T extends Enum<T>, K> {

    private final T type;

    private final K key;

    public TypeKey(T type) {
        this(type, null);
    }

    public TypeKey(T type, K key) {
        this.type = type;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeKey<?, ?> typeKey = (TypeKey<?, ?>) o;
        return type.equals(typeKey.type) && Objects.equals(key, typeKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key);
    }

    public T type() {
        return type;
    }

    public String typeName() {
        return type.name();
    }

    public <U> Optional<U> mapKey(Function<? super K, ? extends U> mapper) {
        return key == null ? Optional.empty() : Optional.of(mapper.apply(key));
    }

    public K key() {
        return key;
    }
}
